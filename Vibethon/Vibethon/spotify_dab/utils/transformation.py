class reuse:
    def dropcol(self, df, col):
        df = df.drop(*col)
        return df