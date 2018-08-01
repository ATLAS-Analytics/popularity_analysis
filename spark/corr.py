import matplotlib.pyplot as plt
from matplotlib import colors
import pandas as pd
import numpy as np
import pyspark.mllib.stat as stat
#######################################################################
#correlation functions

#find correlations using pandas
def corr_pd(df):
    names = df.schema.names
    pd_conv = df.toPandas()
    corr = pd_conv.corr()
    return corr

#find correlations using pyspark
def corr_pys(df):
    features = df.map(lambda row: row[0:])
    corr = stat.Statistics.corr(features, method='pearson')
    return corr

#plot correlation matrix
def plot(df, df_names, filename):
    #set plot size
    fig, ax = plt.subplots(figsize=(10,10)) 
    #set colours
    cmap = plt.cm.get_cmap("plasma",lut=10)
    bounds = np.arange(0.0, 1.1, 0.1)
    norm = colors.BoundaryNorm(bounds, cmap.N)
    
    #plot heatmap
    plot = ax.matshow(np.absolute(df), cmap=cmap, norm=norm)
    #x and y labels
    plt.xticks(range(len(df_names)), df_names, rotation=90);
    plt.yticks(range(len(df_names)), df_names);
    #colorbar
    cbar = fig.colorbar(plot)
    
    for (i, j), z in np.ndenumerate(np.absolute(df)):
        ax.text(j, i, '{:0.2f}'.format(z), ha='center', va='center',
            bbox=dict(boxstyle='round', facecolor='white', edgecolor='0.3'))

    filepath = "./corr_img/" + filename
    fig.savefig(filepath)
    plt.show()
