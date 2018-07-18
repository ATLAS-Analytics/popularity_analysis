import matplotlib.pyplot as plt

dist_file = open('/part-r-00000', 'r')

# read in from file and split into x-values and y-values
x = []
y = []
for line in dist_file:
    line = line.rstrip('\n')
    seperate = line.split('\t')
    x += [int(float(seperate[0]))]
    y += [int(seperate[1])]

#plot settings
f = plt.figure()
plt.title('Frequency of files accessed in 2017')
plt.xlabel('Age of file at time accessed (days)')
plt.ylabel('Frequency')
#plt.xscale('log')
plt.yscale('log')
plt.grid(True)

#plot
plt.bar(x, y)
#plt.ylim(1,50000)
#plt.xlim(0,50000)

f.savefig("dist_by_age_2017.png")
plt.show()
