import matplotlib.pyplot as plt

dist_file = open('file_life/months.txt', 'r')

# read in from file and split into x-values and y-values

for line in dist_file:
    line = line.rstrip('\n')
    split = line.split(',')
    print split
    y = [float(num) for num in split] 

x = range(0, len(y))
#plot settings
f = plt.figure()
plt.title('Lifetime of an average file')
plt.xlabel('Age of file (days)')
plt.ylabel('Average Number of accesses')
#plt.xscale('log')
#plt.yscale('log')
plt.grid(True)

#plot
plt.bar(x, y)
#plt.ylim(1,50000)
#plt.xlim(0,50000)

f.savefig("file_life/months.png")
plt.show()
