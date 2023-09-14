import os
from pathlib import Path

import numpy as np
import pandas as pd
from autogluon.tabular import TabularDataset, TabularPredictor

KNOWN_COLS = [
    # (col_name, legal_feature)
    ("Actual Loops", False),
    ("Actual Input Rows", False),
    ("Actual Rows", False),
    ("Actual Startup Time", False),
    ("Actual Total Time", False),
    ("Alias", True),
    ("Async Capable", True),
    ("Benchmark", False),
    ("Bytejack", False),
    ("Cache Evictions", False),
    ("Cache Hits", False),
    ("Cache Key", True),
    ("Cache Misses", False),
    ("Cache Mode", True),
    ("Cache Overflows", False),  # TODO(WAN)
    ("Command", True),
    ("CTE Name", True),
    ("DefaultTimeout", False),
    ("Disk Usage", False),
    ("Estimated Input Rows", True),
    ("Exact Heap Blocks", False),
    ("Exclude", False),  # For runtime timeout computation.
    ("Experiment", False),
    ("Filter", True),
    ("Full-sort Groups.Group Count", False),
    ("Full-sort Groups.Sort Methods Used", False),
    ("Full-sort Groups.Sort Space Memory.Average Sort Space Used", False),
    ("Full-sort Groups.Sort Space Memory.Peak Sort Space Used", False),
    ("Group Key", False),  # TODO(WAN)
    ("Grouping Sets", False),  # TODO(WAN)
    ("Hash Batches", False),
    ("Hash Buckets", False),
    ("Hash Cond", True),
    ("HashAgg Batches", False),
    ("Heap Fetches", False),
    ("Index Cond", True),
    ("Index Name", True),
    ("Inner Unique", True),
    ("Join Filter", True),
    ("Join Type", True),
    ("Lossy Heap Blocks", False),
    ("Merge Cond", True),
    ("Node Type", True),
    ("Operator Stopped", False),
    ("Operator Time", False),
    ("Original Hash Batches", False),
    ("Original Hash Buckets", False),
    ("Output", False),  # TODO(WAN)
    ("Parallel Aware", True),
    ("Params Evaluated", False),  # TODO(WAN)
    ("Parent Relationship", True),
    ("Partial Mode", True),
    ("Peak Memory Usage", False),
    ("Plan Node Id", True),
    ("Plan Rows", True),
    ("Plan Width", True),
    ("Planned Partitions", True),
    ("Pre-sorted Groups.Group Count", False),
    ("Pre-sorted Groups.Sort Methods Used", False),
    ("Pre-sorted Groups.Sort Space Memory.Average Sort Space Used", False),
    ("Pre-sorted Groups.Sort Space Memory.Peak Sort Space Used", False),
    ("Presorted Key", False),
    ("Query", False),
    ("Query Execution Time", False),
    ("Query Planning Time", False),
    ("Query Total Time", False),
    ("Recheck Cond", True),
    ("Relation Name", True),
    ("Repeatable Seed", True),
    ("Rows Removed by Filter", False),
    ("Rows Removed by Index Recheck", False),
    ("Rows Removed by Join Filter", False),
    ("Sampling Method", True),
    ("Sampling Parameters", False),  # TODO(WAN)
    ("SF", False),
    ("Scan Direction", True),
    ("Schema", True),
    ("Seed", False),
    ("Single Copy", True),
    ("Sort Key", False),  # TODO(WAN)
    ("Sort Method", True),
    ("Sort Space Type", True),
    ("Sort Space Used", False),
    ("Source", False),
    ("Startup Cost", True),
    ("Strategy", True),
    ("Subplan Name", True),
    ("Subplans Removed", False),
    ("Total Cost", True),
    ("Workers Launched", False),
    ("Workers Planned", True),
]

ALL_COLS = [col_name for col_name, _ in KNOWN_COLS]
LEGAL_COLS = [col_name for col_name, legal_feature in KNOWN_COLS if legal_feature]


def generate_metadata(source_file: Path, artifact_root, model_benchmark, model_sf, bytejack):
    curpath = source_file
    metadata = {}
    metadata["Source"] = str(curpath)
    metadata["Query"] = str(curpath.stem)
    curpath = curpath.parent
    metadata["Seed"] = str(curpath.stem)
    curpath = curpath.parent
    if model_benchmark == "dsb":
        # skip over dsb generation type
        curpath = curpath.parent
    metadata["SF"] = str(curpath.stem).split("_")[1]
    curpath = curpath.parent
    metadata["Benchmark"] = str(curpath.stem)
    curpath = curpath.parent
    metadata["Experiment"] = str(curpath.stem)

    if model_benchmark == "tpch":
        metadata["DefaultTimeout"] = Path(artifact_root / f"experiment/default/{str(model_benchmark)}/sf_{str(model_sf)}/{metadata['Seed']}/{metadata['Query']}.timeout").exists()
    else:
        metadata["DefaultTimeout"] = Path(artifact_root / f"experiment/default/{str(model_benchmark)}/sf_{str(model_sf)}/default/{metadata['Seed']}/{metadata['Query']}.timeout").exists()

    metadata["Bytejack"] = bytejack

    return metadata


def shred(source_file: Path, plan_dict: dict, artifact_root, model_benchmark, model_sf, bytejack):
    plan_cur = plan_dict["Plan"]
    query_planning_time = plan_dict["Planning Time"]
    query_execution_time = plan_dict["Execution Time"]
    shreddable = [plan_cur]
    shredded = []

    def has_stopped_somewhere(plan_node):
        if plan_node.get("Operator Stopped", False):
            return True
        for child in plan_node.get("Plans", []):
            if has_stopped_somewhere(child):
                return True
        return False

    while len(shreddable) > 0:
        plan_cur = shreddable.pop()
        skip_cur = False

        for k, v in generate_metadata(source_file, artifact_root, model_benchmark, model_sf, bytejack).items():
            plan_cur[k] = v

        plan_cur["Query Planning Time"] = query_planning_time
        plan_cur["Query Execution Time"] = query_execution_time
        plan_cur["Query Total Time"] = query_planning_time + query_execution_time
        plan_cur["Exclude"] = False

        if "Workers" in plan_cur:
            del plan_cur["Workers"]

        if plan_cur["Node Type"] == "Sample Scan":
            plan_cur["Node Type"] = "Seq Scan"

        estimated_input_rows = 0
        actual_input_rows = 0
        if "Plans" in plan_cur:
            for plan_child in plan_cur["Plans"]:
                estimated_input_rows += plan_child["Plan Rows"]
                actual_input_rows += plan_child["Actual Rows"]
                shreddable.append(plan_child)
            del plan_cur["Plans"]
        plan_cur["Estimated Input Rows"] = estimated_input_rows
        plan_cur["Actual Input Rows"] = actual_input_rows

        if "bytejack" in plan_cur["Experiment"]:
            if plan_cur["Operator Time"] <= 1e-6:
                plan_cur["Operator Time"] = 1e-6

            if has_stopped_somewhere(plan_cur):
                pct_done = max(1, plan_cur["Actual Rows"] / plan_cur["Plan Rows"]) * 100
                # x% done in y seconds = 1% done in y/x seconds
                plan_cur["Operator Time"] = plan_cur["Operator Time"] / pct_done * 100

        if not skip_cur:
            shredded.append(plan_cur)
    return sorted(shredded, key=lambda x: x["Plan Node Id"])


def load_results():
    artifact_root = Path(os.getenv("ARTIFACT_ROOT"))
    model_benchmark = str(os.getenv("MODEL_BENCHMARK"))
    model_sf = str(os.getenv("MODEL_SF"))

    cache_root = artifact_root / "cache"

    cache_root.mkdir(parents=True, exist_ok=True)
    cached_df = cache_root / f"experiment_{str(model_benchmark)}_sf_{str(model_sf)}.pq"
    if cached_df.exists():
        df = pd.read_parquet(cached_df)
        return df

    if model_benchmark == "tpch":
        planfiles = sorted(
            artifact_root.glob(
                f"./experiment/*/{str(model_benchmark)}/sf_{str(model_sf)}/*/*.res"
            )
        )
        toutfiles = sorted(
            artifact_root.glob(
                f"./experiment/*/{str(model_benchmark)}/sf_{str(model_sf)}/*/*.timeout"
            )
        )
    elif model_benchmark == "dsb":
        planfiles = sorted(
            artifact_root.glob(
                f"./experiment/*/{str(model_benchmark)}/sf_{str(model_sf)}/default/*/*.res"
            )
        )
        toutfiles = sorted(
            artifact_root.glob(
                f"./experiment/*/{str(model_benchmark)}/sf_{str(model_sf)}/default/*/*.timeout"
            )
        )

    operators = []
    for planfile in planfiles:
        assert (
            planfile.parent / f"{planfile.stem}.ok"
        ).exists(), f".ok file missing for: {planfile}"
        with open(planfile) as f:
            contents = "".join(f.readlines())
        if len(contents) == 0:
            continue
        bytejack = contents.startswith(r"{'Bytejack': 'true',")
        j = eval(contents)
        operators.extend(shred(planfile, j, artifact_root, model_benchmark, model_sf, bytejack))

    for toutfile in toutfiles:
        assert (
            planfile.parent / f"{planfile.stem}.ok"
        ).exists(), f".ok file missing for: {planfile}"
        metadata = generate_metadata(toutfile, artifact_root, model_benchmark, model_sf, False)
        metadata["Query Total Time"] = 5 * 60 * 1000  # timeout value
        metadata["Exclude"] = True
        operators.append(metadata)

    df = pd.json_normalize(operators)

    assert set(df.columns.unique()).issubset(
        set(ALL_COLS)
    ), f"Unknown columns? {df.columns.unique().difference(set(ALL_COLS))}"
    df.to_parquet(cached_df)
    df = pd.read_parquet(cached_df)
    return df


def main():
    artifact_root = Path(os.getenv("ARTIFACT_ROOT"))
    model_benchmark = str(os.getenv("MODEL_BENCHMARK"))
    model_sf = str(os.getenv("MODEL_SF"))

    df = load_results()

    if model_benchmark == "tpch":
        seeds = df["Seed"].unique()
        rng = np.random.default_rng(15721)
        train_seeds = sorted(
            rng.choice(seeds, size=int(0.8 * len(seeds)), replace=False).tolist()
        )
        test_seeds = sorted(list(set(seeds) - set(train_seeds)))
    elif model_benchmark == "dsb":
        train_seeds = ["15721"]
        test_seeds = ["15722"]
    else:
        raise Exception

    model_root = artifact_root / "model" / f"{str(model_benchmark)}_sf_{str(model_sf)}"
    model_root.mkdir(parents=True, exist_ok=True)
    model_benchmark = str(model_benchmark)
    model_sf = str(model_sf)

    target_col = "Operator Time"
    test_df = df[
        (df["Seed"].isin(test_seeds))
        & (df["Experiment"] == "default")
        & (df["Benchmark"] == model_benchmark)
        & (df["SF"] == model_sf)
        & (df["Exclude"] == False)
    ]
    eval_df = test_df.copy()

    expts = []
    for expt in df[(df["Benchmark"] == model_benchmark) & (df["SF"] == model_sf)][
        "Experiment"
    ].unique():
        model_path = (model_root / expt).absolute()

        train_df = df[
            (df["Seed"].isin(train_seeds))
            & (df["Experiment"] == expt)
            & (df["Benchmark"] == model_benchmark)
            & (df["SF"] == model_sf)
            & (df["Exclude"] == False)
        ]

        cols = [c for c in LEGAL_COLS if c in df.columns.unique()] + [target_col]
        train_data = TabularDataset(train_df[cols])
        test_data = TabularDataset(test_df[cols])
        print(f"{LEGAL_COLS=} {target_col=}")
        print(f"{train_data.shape=} {test_data.shape=}")

        if not model_path.exists():
            predictor = TabularPredictor(
                label=target_col,
                path=str(model_path),
                eval_metric="mean_absolute_error",
            )
            predictor.fit(train_data, time_limit=60 * 5)
        predictor = TabularPredictor.load(str(model_path))

        eval_df[f"Predicted_{expt}"] = predictor.predict(
            test_data.drop(columns=[target_col])
        )
        eval_df[f"Absolute Error_{expt}"] = (
            eval_df[target_col] - eval_df[f"Predicted_{expt}"]
        ).abs()
        expts.append(expt)

    benchmark_sf_df = df[(df["Benchmark"] == model_benchmark) & (df["SF"] == model_sf)]

    runtime = benchmark_sf_df.pivot_table(
        values=["Query Total Time"],
        index=["Query", "Seed"],
        columns=["Experiment"],
        aggfunc="first",
    ).droplevel(0, axis=1)
    runtime.to_csv(model_root / "runtime_query_seed.csv")
    runtime.groupby(level=0).sum().to_csv(model_root / "runtime_query.csv")
    runtime.sum().to_csv(model_root / "runtime.csv")

    prediction = eval_df.pivot_table(
        values=["Operator Time", *[f"Predicted_{expt}" for expt in expts]],
        index=["Query", "Seed"],
        columns=["Experiment"],
        aggfunc="sum",
    )
    prediction.to_csv(model_root / "prediction_query_seed.csv")
    prediction.groupby(level=0).sum().to_csv(model_root / "prediction_query.csv")
    prediction.sum().to_csv(model_root / "prediction.csv")

    abs_err_data = {}
    for a, b in prediction:
        assert b == "default"
        if a == "Operator Time":
            continue
        expt_name = a[len("Predicted_") :]
        abs_err_data[expt_name] = (
            prediction[(a, b)] - prediction[("Operator Time", "default")]
        ).abs()
    abs_err_df = pd.DataFrame(abs_err_data)
    abs_err_df.to_csv(model_root / "error_query_seed.csv")
    abs_err_df.groupby(level=0).sum().to_csv(model_root / "error_query.csv")
    abs_err_df.sum().to_csv(model_root / "error.csv")

    (runtime.sum() / 1000).to_frame("Runtime (s)").join(
        abs_err_df.mean().to_frame("MAE (s)")
    ).sort_values(by=["MAE (s)", "Runtime (s)"]).to_csv(model_root / "summary.csv")

    # breakpoint()


if __name__ == "__main__":
    main()
