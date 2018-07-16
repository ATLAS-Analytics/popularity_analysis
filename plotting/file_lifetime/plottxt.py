import matplotlib.pyplot as plt

dist_file = open('weeks.txt', 'r')

# read in from file and split into x-values and y-values
x = range(0,15)

for line in dist_file:
    line = line.rstrip('\n')
    split = line.split(',')
    print split
    y = [float(num) for num in split] 
print x, y
print len(x), len(y)
#plot settings
f = plt.figure()
plt.title('Lifetime of an average file')
plt.xlabel('Age of file (weeks)')
plt.ylabel('Average Number of accesses')
#plt.xscale('log')
#plt.yscale('log')
plt.grid(True)

#plot
plt.bar(x, y)
#plt.ylim(1,50000)
#plt.xlim(0,50000)

f.savefig("weeks.png")
plt.show()
