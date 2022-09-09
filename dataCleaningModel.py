import pandas as pd
import numpy as np


class DataCleaning:
    def __init__(self, data):
        self.df = data

    def showHead(self, rows):
        return print(self.df.head(rows))

    def toDateTime(self, column):
        self.df[column] = pd.to_datetime(self.df[column])

    def fillNa(self, column):
        self.df[column] = self.df[column].fillna(0)

    def fillNaText(self, column,text):
        self.df[column] = self.df[column].fillna(text)

    def dropColumn(self, column):
        self.df.drop([column], axis=1, inplace=True)

    def groupByCount(self, col1, col2):
        return self.df.groupby([col1], as_index=False)[col2].count()

    def uniqueCount(self, column):
        return len(pd.unique(self.df[column]))

    def uniques(self, column):
        return pd.unique(self.df[column])

    def nullNumber(self):
        return self.df.isna().sum()

    def SumColumn(self, col1):
        return self.df[col1].sum()