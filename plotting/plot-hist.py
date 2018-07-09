import matplotlib.pyplot as plt
import numpy as np

dist_file = open('dist_by_age/part-r-00000', 'r')

# read in from file and split into x-values and y-values
age = []
counts = []
for line in dist_file:
    line = line.rstrip('\n')
    seperate = line.split('\t')
    age += [int(seperate[0])]
    counts += [int(seperate[1])]

binNum = np.sqrt(len(age))
ageMin = min(age)
ageMax = max(age)
print binNum
print ageMin
print ageMax

bins = [int((x-ageMin)*binNum/(ageMax-ageMin)) for x in age]

#plot settings
f = plt.figure()
plt.xlabel('Number of dataset accesses')
plt.ylabel('Count')
#plt.xscale('log')
#plt.yscale('log')
plt.grid(True)

#plot
#plt.bar(datasetCount, counts, 1.0, color='g')
plt.hist(age, bins)

#plt.ylim(1,50000)
#plt.xlim(0,50000)

plt.show()
#f.savefig("access_by_age.png")
