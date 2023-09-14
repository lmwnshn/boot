from abc import ABC


class Rewriter(ABC):
    def rewrite(self, query_num: int, query_subnum: int, query: str) -> (str, bool):
        raise NotImplementedError("No base implementation.")


class EARewriter(Rewriter):
    def rewrite(self, query_num: int, query_subnum: int, query: str) -> (str, bool):
        is_ea = False
        if query[:10].lower().strip().startswith("select"):
            is_ea, query = True, f"EXPLAIN (ANALYZE, FORMAT JSON, VERBOSE) {query}"
        return query, is_ea


class NTSRewriter(EARewriter):
    def __init__(self, sample_method: str, sample_seed: str):
        super().__init__()
        self.sample_method = sample_method
        self.sample_seed = sample_seed

    def rewrite(self, query_num: int, query_subnum: int, query: str) -> (str, bool):
        query, is_ea = super().rewrite(query_num, query_subnum, query)
        if not is_ea:
            return query, is_ea

        sample_method = self.sample_method
        sample_seed = self.sample_seed
        if query_num == 1:
            query = query.replace(
                "\n\tlineitem",
                f"\n\tlineitem TABLESAMPLE {sample_method} {sample_seed}",
            )
        elif query_num == 2:
            query = query.replace(
                "\n\tpart,", f"\n\tpart TABLESAMPLE {sample_method} {sample_seed},"
            )
            query = query.replace(
                "\n\tsupplier,",
                f"\n\tsupplier TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\tpartsupp,",
                f"\n\tpartsupp TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\tnation,", f"\n\tnation TABLESAMPLE {sample_method} {sample_seed},"
            )
            query = query.replace(
                "\n\tregion", f"\n\tregion TABLESAMPLE {sample_method} {sample_seed}"
            )
            query = query.replace(
                "\n\t\t\tpart,",
                f"\n\t\t\tpart TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\tsupplier,",
                f"\n\t\t\tsupplier TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\tpartsupp,",
                f"\n\t\t\tpartsupp TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\tnation,",
                f"\n\t\t\tnation TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\tregion",
                f"\n\t\t\tregion TABLESAMPLE {sample_method} {sample_seed}",
            )
        elif query_num == 3:
            query = query.replace(
                "\n\tcustomer,",
                f"\n\tcustomer TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\torders,", f"\n\torders TABLESAMPLE {sample_method} {sample_seed},"
            )
            query = query.replace(
                "\n\tlineitem",
                f"\n\tlineitem TABLESAMPLE {sample_method} {sample_seed}",
            )
        elif query_num == 4:
            query = query.replace(
                "\n\torders", f"\n\torders TABLESAMPLE {sample_method} {sample_seed}"
            )
            query = query.replace(
                "\n\t\t\tlineitem",
                f"\n\t\t\tlineitem TABLESAMPLE {sample_method} {sample_seed}",
            )
        elif query_num == 5:
            query = query.replace(
                "\n\tcustomer,",
                f"\n\tcustomer TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\torders,", f"\n\torders TABLESAMPLE {sample_method} {sample_seed},"
            )
            query = query.replace(
                "\n\tlineitem,",
                f"\n\tlineitem TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\tsupplier,",
                f"\n\tsupplier TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\tnation,", f"\n\tnation TABLESAMPLE {sample_method} {sample_seed},"
            )
            query = query.replace(
                "\n\tregion", f"\n\tregion TABLESAMPLE {sample_method} {sample_seed}"
            )
        elif query_num == 6:
            query = query.replace(
                "\n\tlineitem",
                f"\n\tlineitem TABLESAMPLE {sample_method} {sample_seed}",
            )
        elif query_num == 7:
            query = query.replace(
                "\n\t\t\tsupplier,",
                f"\n\t\t\tsupplier TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\tlineitem,",
                f"\n\t\t\tlineitem TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\torders,",
                f"\n\t\t\torders TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\tcustomer,",
                f"\n\t\t\tcustomer TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\tnation n1,",
                f"\n\t\t\tnation n1 TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\tnation n2",
                f"\n\t\t\tnation n2 TABLESAMPLE {sample_method} {sample_seed}",
            )
        elif query_num == 8:
            query = query.replace(
                "\n\t\t\tpart,",
                f"\n\t\t\tpart TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\tsupplier,",
                f"\n\t\t\tsupplier TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\tlineitem,",
                f"\n\t\t\tlineitem TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\torders,",
                f"\n\t\t\torders TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\tcustomer,",
                f"\n\t\t\tcustomer TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\tnation n1,",
                f"\n\t\t\tnation n1 TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\tnation n2,",
                f"\n\t\t\tnation n2 TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\tregion",
                f"\n\t\t\tregion TABLESAMPLE {sample_method} {sample_seed}",
            )
        elif query_num == 9:
            query = query.replace(
                "\n\t\t\tpart,",
                f"\n\t\t\tpart TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\tsupplier,",
                f"\n\t\t\tsupplier TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\tlineitem,",
                f"\n\t\t\tlineitem TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\tpartsupp,",
                f"\n\t\t\tpartsupp TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\torders,",
                f"\n\t\t\torders TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\tnation",
                f"\n\t\t\tnation TABLESAMPLE {sample_method} {sample_seed}",
            )
        elif query_num == 10:
            query = query.replace(
                "\n\tcustomer,",
                f"\n\tcustomer TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\torders,", f"\n\torders TABLESAMPLE {sample_method} {sample_seed},"
            )
            query = query.replace(
                "\n\tlineitem,",
                f"\n\tlineitem TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\tnation", f"\n\tnation TABLESAMPLE {sample_method} {sample_seed}"
            )
        elif query_num == 11:
            query = query.replace(
                "\n\tpartsupp,",
                f"\n\tpartsupp TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\tsupplier,",
                f"\n\tsupplier TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\tnation", f"\n\tnation TABLESAMPLE {sample_method} {sample_seed}"
            )
            query = query.replace(
                "\n\t\t\t\tpartsupp,",
                f"\n\t\t\t\tpartsupp TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\t\tsupplier,",
                f"\n\t\t\t\tsupplier TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\t\tnation",
                f"\n\t\t\t\tnation TABLESAMPLE {sample_method} {sample_seed}",
            )
        elif query_num == 12:
            query = query.replace(
                "\n\torders,", f"\n\torders TABLESAMPLE {sample_method} {sample_seed},"
            )
            query = query.replace(
                "\n\tlineitem",
                f"\n\tlineitem TABLESAMPLE {sample_method} {sample_seed}",
            )
        elif query_num == 13:
            query = query.replace(
                "\n\t\t\tcustomer left outer join orders",
                f"\n\t\t\tcustomer TABLESAMPLE {sample_method} {sample_seed} left outer join orders TABLESAMPLE {sample_method} {sample_seed}",
            )
        elif query_num == 14:
            query = query.replace(
                "\n\tlineitem,",
                f"\n\tlineitem TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\tpart", f"\n\tpart TABLESAMPLE {sample_method} {sample_seed}"
            )
        elif (query_num, query_subnum) == (15, 1):
            query = query.replace(
                "\n\t\tlineitem",
                f"\n\t\tlineitem TABLESAMPLE {sample_method} {sample_seed}",
            )
        elif (query_num, query_subnum) == (15, 2):
            query = query.replace(
                "\n\tsupplier,",
                f"\n\tsupplier TABLESAMPLE {sample_method} {sample_seed},",
            )
        elif query_num == 16:
            query = query.replace(
                "\n\tpartsupp,",
                f"\n\tPARTSUPP TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\tpart", f"\n\tpart TABLESAMPLE {sample_method} {sample_seed}"
            )
            query = query.replace("PARTSUPP", "partsupp")
            query = query.replace(
                "\n\t\t\tsupplier",
                f"\n\t\t\tsupplier TABLESAMPLE {sample_method} {sample_seed}",
            )
        elif query_num == 17:
            query = query.replace(
                "\n\tlineitem,",
                f"\n\tlineitem TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\tpart", f"\n\tpart TABLESAMPLE {sample_method} {sample_seed}"
            )
            query = query.replace(
                "\n\t\t\tlineitem",
                f"\n\t\t\tlineitem TABLESAMPLE {sample_method} {sample_seed}",
            )
        elif query_num == 18:
            query = query.replace(
                "\n\tcustomer,",
                f"\n\tcustomer TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\torders,", f"\n\torders TABLESAMPLE {sample_method} {sample_seed},"
            )
            query = query.replace(
                "\n\tlineitem",
                f"\n\tlineitem TABLESAMPLE {sample_method} {sample_seed}",
            )
            query = query.replace(
                "\n\t\t\tlineitem",
                f"\n\t\t\tlineitem TABLESAMPLE {sample_method} {sample_seed}",
            )
        elif query_num == 19:
            query = query.replace(
                "\n\tlineitem,",
                f"\n\tlineitem TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\tpart", f"\n\tpart TABLESAMPLE {sample_method} {sample_seed}"
            )
        elif query_num == 20:
            query = query.replace(
                "\n\tsupplier,",
                f"\n\tsupplier TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\tnation", f"\n\tnation TABLESAMPLE {sample_method} {sample_seed}"
            )
            query = query.replace(
                "\n\t\t\tpartsupp",
                f"\n\t\t\tpartsupp TABLESAMPLE {sample_method} {sample_seed}",
            )
            query = query.replace(
                "\n\t\t\t\t\tpart",
                f"\n\t\t\t\t\tpart TABLESAMPLE {sample_method} {sample_seed}",
            )
            query = query.replace(
                "\n\t\t\t\t\tlineitem",
                f"\n\t\t\t\t\tlineitem TABLESAMPLE {sample_method} {sample_seed}",
            )
        elif query_num == 21:
            query = query.replace(
                "\n\tsupplier,",
                f"\n\tsupplier TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\tlineitem l1,",
                f"\n\tlineitem l1 TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\torders,", f"\n\torders TABLESAMPLE {sample_method} {sample_seed},"
            )
            query = query.replace(
                "\n\tnation", f"\n\tnation TABLESAMPLE {sample_method} {sample_seed}"
            )
            query = query.replace(
                "\n\t\t\tlineitem l2",
                f"\n\t\t\tlineitem l2 TABLESAMPLE {sample_method} {sample_seed}",
            )
            query = query.replace(
                "\n\t\t\tlineitem l3",
                f"\n\t\t\tlineitem l3 TABLESAMPLE {sample_method} {sample_seed}",
            )
        elif query_num == 22:
            query = query.replace(
                "\n\t\t\tcustomer",
                f"\n\t\t\tcustomer TABLESAMPLE {sample_method} {sample_seed}",
            )
            query = query.replace(
                "\n\t\t\t\t\tcustomer",
                f"\n\t\t\t\t\tcustomer TABLESAMPLE {sample_method} {sample_seed}",
            )
            query = query.replace(
                "\n\t\t\t\t\torders",
                f"\n\t\t\t\t\torders TABLESAMPLE {sample_method} {sample_seed}",
            )
        return query, is_ea


class STSRewriter(EARewriter):
    def __init__(self, sample_method: str, sample_seed: str):
        super().__init__()
        self.sample_method = sample_method
        self.sample_seed = sample_seed

    def rewrite(self, query_num: int, query_subnum: int, query: str) -> (str, bool):
        query, is_ea = super().rewrite(query_num, query_subnum, query)
        if not is_ea:
            return query, is_ea

        sample_method = self.sample_method
        sample_seed = self.sample_seed
        if query_num == 1:
            query = query.replace(
                "\n\tlineitem",
                f"\n\tlineitem TABLESAMPLE {sample_method} {sample_seed}",
            )
        elif query_num == 2:
            query = query.replace(
                "\n\tpart,", f"\n\tpart TABLESAMPLE {sample_method} {sample_seed},"
            )
            query = query.replace(
                "\n\tsupplier,",
                f"\n\tsupplier TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\tpartsupp,",
                f"\n\tpartsupp TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\tnation,", f"\n\tnation TABLESAMPLE {sample_method} {sample_seed},"
            )
            query = query.replace(
                "\n\tregion", f"\n\tregion TABLESAMPLE {sample_method} {sample_seed}"
            )
        elif query_num == 3:
            query = query.replace(
                "\n\tcustomer,",
                f"\n\tcustomer TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\torders,", f"\n\torders TABLESAMPLE {sample_method} {sample_seed},"
            )
            query = query.replace(
                "\n\tlineitem",
                f"\n\tlineitem TABLESAMPLE {sample_method} {sample_seed}",
            )
        elif query_num == 4:
            query = query.replace(
                "\n\torders", f"\n\torders TABLESAMPLE {sample_method} {sample_seed}"
            )
        elif query_num == 5:
            query = query.replace(
                "\n\tcustomer,",
                f"\n\tcustomer TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\torders,", f"\n\torders TABLESAMPLE {sample_method} {sample_seed},"
            )
            query = query.replace(
                "\n\tlineitem,",
                f"\n\tlineitem TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\tsupplier,",
                f"\n\tsupplier TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\tnation,", f"\n\tnation TABLESAMPLE {sample_method} {sample_seed},"
            )
            query = query.replace(
                "\n\tregion", f"\n\tregion TABLESAMPLE {sample_method} {sample_seed}"
            )
        elif query_num == 6:
            query = query.replace(
                "\n\tlineitem",
                f"\n\tlineitem TABLESAMPLE {sample_method} {sample_seed}",
            )
        elif query_num == 7:
            query = query.replace(
                "\n\t\t\tsupplier,",
                f"\n\t\t\tsupplier TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\tlineitem,",
                f"\n\t\t\tlineitem TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\torders,",
                f"\n\t\t\torders TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\tcustomer,",
                f"\n\t\t\tcustomer TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\tnation n1,",
                f"\n\t\t\tnation n1 TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\tnation n2",
                f"\n\t\t\tnation n2 TABLESAMPLE {sample_method} {sample_seed}",
            )
        elif query_num == 8:
            query = query.replace(
                "\n\t\t\tpart,",
                f"\n\t\t\tpart TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\tsupplier,",
                f"\n\t\t\tsupplier TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\tlineitem,",
                f"\n\t\t\tlineitem TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\torders,",
                f"\n\t\t\torders TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\tcustomer,",
                f"\n\t\t\tcustomer TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\tnation n1,",
                f"\n\t\t\tnation n1 TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\tnation n2,",
                f"\n\t\t\tnation n2 TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\tregion",
                f"\n\t\t\tregion TABLESAMPLE {sample_method} {sample_seed}",
            )
        elif query_num == 9:
            query = query.replace(
                "\n\t\t\tpart,",
                f"\n\t\t\tpart TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\tsupplier,",
                f"\n\t\t\tsupplier TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\tlineitem,",
                f"\n\t\t\tlineitem TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\tpartsupp,",
                f"\n\t\t\tpartsupp TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\torders,",
                f"\n\t\t\torders TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\t\t\tnation",
                f"\n\t\t\tnation TABLESAMPLE {sample_method} {sample_seed}",
            )
        elif query_num == 10:
            query = query.replace(
                "\n\tcustomer,",
                f"\n\tcustomer TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\torders,", f"\n\torders TABLESAMPLE {sample_method} {sample_seed},"
            )
            query = query.replace(
                "\n\tlineitem,",
                f"\n\tlineitem TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\tnation", f"\n\tnation TABLESAMPLE {sample_method} {sample_seed}"
            )
        elif query_num == 11:
            query = query.replace(
                "\n\tpartsupp,",
                f"\n\tpartsupp TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\tsupplier,",
                f"\n\tsupplier TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\tnation", f"\n\tnation TABLESAMPLE {sample_method} {sample_seed}"
            )
        elif query_num == 12:
            query = query.replace(
                "\n\torders,", f"\n\torders TABLESAMPLE {sample_method} {sample_seed},"
            )
            query = query.replace(
                "\n\tlineitem",
                f"\n\tlineitem TABLESAMPLE {sample_method} {sample_seed}",
            )
        elif query_num == 13:
            query = query.replace(
                "\n\t\t\tcustomer left outer join orders",
                f"\n\t\t\tcustomer TABLESAMPLE {sample_method} {sample_seed} left outer join orders TABLESAMPLE {sample_method} {sample_seed}",
            )
        elif query_num == 14:
            query = query.replace(
                "\n\tlineitem,",
                f"\n\tlineitem TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\tpart", f"\n\tpart TABLESAMPLE {sample_method} {sample_seed}"
            )
        elif (query_num, query_subnum) == (15, 1):
            query = query.replace(
                "\n\t\tlineitem",
                f"\n\t\tlineitem TABLESAMPLE {sample_method} {sample_seed}",
            )
        elif (query_num, query_subnum) == (15, 2):
            query = query.replace(
                "\n\tsupplier,",
                f"\n\tsupplier TABLESAMPLE {sample_method} {sample_seed},",
            )
        elif query_num == 16:
            query = query.replace(
                "\n\tpartsupp,",
                f"\n\tPARTSUPP TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\tpart", f"\n\tpart TABLESAMPLE {sample_method} {sample_seed}"
            )
            query = query.replace("PARTSUPP", "partsupp")
        elif query_num == 17:
            query = query.replace(
                "\n\tlineitem,",
                f"\n\tlineitem TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\tpart", f"\n\tpart TABLESAMPLE {sample_method} {sample_seed}"
            )
        elif query_num == 18:
            query = query.replace(
                "\n\tcustomer,",
                f"\n\tcustomer TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\torders,", f"\n\torders TABLESAMPLE {sample_method} {sample_seed},"
            )
            query = query.replace(
                "\n\tlineitem",
                f"\n\tlineitem TABLESAMPLE {sample_method} {sample_seed}",
            )
        elif query_num == 19:
            query = query.replace(
                "\n\tlineitem,",
                f"\n\tlineitem TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\tpart", f"\n\tpart TABLESAMPLE {sample_method} {sample_seed}"
            )
        elif query_num == 20:
            query = query.replace(
                "\n\tsupplier,",
                f"\n\tsupplier TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\tnation", f"\n\tnation TABLESAMPLE {sample_method} {sample_seed}"
            )
        elif query_num == 21:
            query = query.replace(
                "\n\tsupplier,",
                f"\n\tsupplier TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\tlineitem l1,",
                f"\n\tlineitem l1 TABLESAMPLE {sample_method} {sample_seed},",
            )
            query = query.replace(
                "\n\torders,", f"\n\torders TABLESAMPLE {sample_method} {sample_seed},"
            )
            query = query.replace(
                "\n\tnation", f"\n\tnation TABLESAMPLE {sample_method} {sample_seed}"
            )
        elif query_num == 22:
            query = query.replace(
                "\n\t\t\tcustomer",
                f"\n\t\t\tcustomer TABLESAMPLE {sample_method} {sample_seed}",
            )
        return query, is_ea
