import matplotlib.pyplot as plt
import numpy as np

def plot_corr(corr_array):
    corr_array.as_matrix()

    fig, ax = plt.subplots()
    im = ax.imshow(corr_array)

    # We want to show all ticks...
    ax.set_xticks(np.arange(len(corr_vars)))
    ax.set_yticks(np.arange(len(corr_vars)))
    # ... and label them with the respective list entries
    ax.set_xticklabels(corr_vars)
    ax.set_yticklabels(corr_vars)

    # Rotate the tick labels and set their alignment.
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right",
         rotation_mode="anchor")

    # Loop over data dimensions and create text annotations.
    for i in range(len(corr_vars)):
        for j in range(len(corr_vars)):
            text = ax.text(j, i, abs(corr_array[i, j]),
                           ha="center", va="center", color="w")
    ax.set_title("Correlation matrix from 2017")
    fig.tight_layout()
    plt.show()
    return 0
