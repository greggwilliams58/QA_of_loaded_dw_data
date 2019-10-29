import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

def plot_the_data(df, FNum, FName):

    variables = list(df.columns)

    #max_period = df.idx.max()

    #print(max_period)

    print(f"column headers for {FNum}_{FName}")
    print(variables)
    

    figplot = df.boxplot(column=variables)

    figplot.set_title(f"Plot of {FNum}_{FName}")

    plt.savefig(f"Plot of  {FNum}_{FName}")
