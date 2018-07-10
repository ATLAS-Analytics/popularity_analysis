import matplotlib.pyplot as plt

dist_file = open('age_distribution/dist_by_age_month_test/part-r-00000', 'r')

# read in from file and split into x-values and y-values
age = []
counts = []
for line in dist_file:
    line = line.rstrip('\n')
    seperate = line.split('\t')
    age += [int(float(seperate[0]))]
    counts += [int(seperate[1])]

#plot settings
f = plt.figure()
plt.xlabel('Age of file')
plt.ylabel('Frequency')
#plt.xscale('log')
#plt.yscale('log')
plt.grid(True)

#plot
plt.bar(age, counts)
#plt.ylim(1,50000)
#plt.xlim(0,50000)

f.savefig("age_distribution/access_by_age_month_test.png")
plt.show()
