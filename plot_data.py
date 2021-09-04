import matplotlib.pyplot as plt
import sys, math

def mean_and_std_dev(data):
    n = len(data)
    mu = sum(data) / n
    sigma = math.sqrt(sum([(x - mu)**2 for x in data]) / (n - 1))
    return mu, sigma

with open('results.out') as f:
    content = f.readlines()

content = [x.strip() for x in content]

count = 0
print(len(content))
# threads, hashbits, average time (2 outliers removed, 1 in each direction)
means = {algo: {t: {hb: 0 for hb in range(1,19)} for t in [1,2,4,8,16,32]} for algo in ['parallel', 'concurrent']}
std_devs = {algo: {t: {hb: 0 for hb in range(1,19)} for t in [1,2,4,8,16,32]} for algo in ['parallel', 'concurrent']}

throughputs = []
algorithm = ''
threads = 0
hashbits = 0
for line in content:
    if count == 0:
        throughputs = []
        line = line.split(' ')
        algorithm = line[0]
        hashbits = line[2]
        threads = line[3]
        #print(f'{algorithm} {hashbits} {threads}')
    else:
        line = line.split(' ')
        throughput = float(line[3])
        throughputs.append(throughput)
    count = count + 1
    if count == 11:
        throughputs.sort()
        throughputs = throughputs[1:len(throughputs) - 1]
        mean, std_dev = mean_and_std_dev(throughputs)
        means[algorithm][int(threads)][int(hashbits)] = mean
        std_devs[algorithm][int(threads)][int(hashbits)] = std_dev
        count = 0

x_vals = range(1,19)
fig, ax = plt.subplots(1,2,figsize=(24,8))

data = None
if sys.argv[1] == 'mean':
    data = means
elif sys.argv[1] == 'std_dev':
    data = std_devs
else:
    exit(0)

# concurrent
for threads in data['concurrent']:
    data_thread = data['concurrent'][threads]
    data_hashbits = [data_thread[x] for x in x_vals]
    ax[0].plot(x_vals, data_hashbits, '.-', label=f'{threads} thread(s)')

ax[0].set_ylim([0,180])
ax[0].set_xlim([1,18])
ax[0].set_xticks(x_vals)
ax[0].grid()
ax[0].set_xlabel('Hash bits')
ax[0].set_ylabel('Millions of tuples/second')
ax[0].legend(loc='best')
ax[0].set_title(f'Concurrent - {sys.argv[1]}',fontsize=18)

# parallel
for threads in data['parallel']:
    data_thread = data['parallel'][threads]
    data_hashbits = [data_thread[x] for x in x_vals]
    ax[1].plot(x_vals, data_hashbits, '.-', label=f'{threads} thread(s)')

ax[1].set_ylim([0,180])
ax[1].set_xlim([1,18])
ax[1].set_xticks(x_vals)
ax[1].grid()
ax[1].set_xlabel('Hash bits')
ax[1].set_ylabel('Millions of tuples/second')
ax[1].legend(loc='best')
ax[1].set_title(f'Parallel buffers - {sys.argv[1]}',fontsize=18)

plt.show()