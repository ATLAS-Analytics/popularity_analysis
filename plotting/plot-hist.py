import matplotlib.pyplot as plt
import time

dist_file = open('last_acc/part-r-00000', 'r')

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
plt.title('Last access date of all files retrieved in 2018-06')
plt.xlabel('Day of month')
plt.ylabel('Frequency')
#plt.xscale('log')
#plt.yscale('log')
plt.grid(True)

#plot
plt.bar(x, y)
#plt.ylim(1,50000)
#plt.xlim(0,50000)

f.savefig("last_acc.png")
plt.show()
