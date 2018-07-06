import matplotlib.pyplot as plt

dist_file = open('download_dist_long/part-r-00000', 'r')

# read in from file and split into x-values and y-values
datasetCount = []
counts = []
for line in dist_file:
    line = line.rstrip('\n')
    seperate = line.split('\t')
    datasetCount += [int(seperate[0])]
    counts += [int(seperate[1])]

#plot settings
f = plt.figure()
plt.xlabel('Number of dataset accesses')
plt.ylabel('Count')
plt.xscale('log')
plt.yscale('log')
plt.grid(True)

#plot
#plt.bar(datasetCount, counts, 1.0, color='g')
plt.plot(datasetCount, counts, marker='o', linewidth=0)

#plt.ylim(1,50000)
#plt.xlim(0,50000)

plt.show()
f.savefig("access_distribution.png")
