import matplotlib.pyplot as plt

datasetCount = []
counts = []


for i in range(0,4):
    print i
    path = 'scatter/not_ganga/part-r-0000{}'.format(i)
    dist_file = open(path, 'r')

# read in from file and split into x-values and y-values
    for line in dist_file:
        line = line.rstrip('\n')
        seperate = line.split('\t')
        datasetCount += [float(seperate[0])]
        counts += [int(seperate[1])]

#plot settings
f = plt.figure()
plt.xlabel('Maximum time difference between dataset accesses (days)')
plt.ylabel('Number of accesses for that dataset')
#plt.xscale('log')
plt.yscale('log')
plt.grid(True)

#plot
#plt.bar(datasetCount, counts, 1.0, color='g')
plt.plot(datasetCount, counts, marker='o', linewidth=0)

#plt.ylim(1,50000)
#plt.xlim(0,50000)

plt.show()
#f.savefig("scatter/not_ganga.png")
