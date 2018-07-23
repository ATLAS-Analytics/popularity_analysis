import matplotlib.pyplot as plt

dist_file = open('time_between/last/ganga/part-r-00000', 'r')

# read in from file and split into x-values and y-values
x = []
y = []
for line in dist_file:
    line = line.rstrip('\n')
    seperate = line.split('\t')
    x += [int(float(seperate[0]))]
    y += [int(seperate[1])]

#plot settings
f = plt.figure(figsize=(10,10))
plt.title('Working out distribution of maximum time between accesses of a dataset 2017')
plt.xlabel('Max time between accesses (days)')
plt.ylabel('Frequency')
#plt.xscale('log')
#plt.yscale('log')
plt.grid(True)

#plot
plt.plot(x, y)
#plt.ylim(1,50000)
#plt.xlim(0,50000)

f.savefig("time_between/last/time_between_ganga_not_log.png")
plt.show()
