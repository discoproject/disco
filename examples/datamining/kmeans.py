import marshal
import disco.core

def map_init(iter, params):
	random.seed()

def random_init_map(e, params):
	return [(random.randint(0,params.k-1),e[1])]

def estimate_map(e, params):
	return[(min([(params.dist(c,e[1]),i) for (i,c) in params.centers.iteritems()])[1],e[1])]

def estimate_combiner(k, v, centers, done, params):
	if done: return [(k,marshal.dumps(v)) for (k,v) in centers.iteritems()]
	params.update_center(centers, k, [1,v])

def estimate_reduce(iter, out, params):
        centers={}
        for k,v in iter:
		params.update_center(centers,int(k),marshal.loads(v))

        for k,v in centers.iteritems():
		out.add(k,marshal.dumps(params.get_center(v)))

def predict_map(e, params):
	return [(e[0],min([(params.dist(c,e),i) for (i,c) in enumerate(params.centers.iteritems())])[1])]

def d2(x,y): return sum([(x[i]-y[i])**2 for i in range(len(x))])

def update_center(centers, k, v):
	if not centers.has_key(k): centers[k]=[0,[0.0]*len(v[1])]

	for i in range(len(v[1])): centers[k][1][i]+=v[1][i]
	centers[k][0]+=v[0]

def get_center(center):
	return [center[0],[x/center[0] for x in center[1]]]

def estimate(input, k, centers=None, iterations=10, master="disco://discomaster", map_reader=disco.func.chain_reader, nr_reduces=None):
	def get_centers(k,job):
		results=job.wait()
		centers={}
		counts={}
		for key,value in disco.core.result_iterator(results):
			v=marshal.loads(value)
			counts[int(key)]=v[0]
			centers[int(key)]=v[1]
		job.purge()
		return counts,centers

	if centers!=None: k=len(centers)
	if nr_reduces==None: nr_reduces=k

	if centers==None:	
		counts,centers = get_centers(k, disco.core.Disco(master).new_job(
			name = 'kmeans_init',
			input = input, 
			map_reader = map_reader, 
			map_init = map_init,
			map = random_init_map, 
			combiner = estimate_combiner, 
			reduce = estimate_reduce,
			nr_reduces = nr_reduces, 
			required_modules=['random'],
			params = disco.core.Params(k=k,update_center=update_center,get_center=get_center)))

	print 'initial cluster sizes:', ' '.join("%d:%d" %(k,v) for k,v in counts.iteritems())

	for i in range(iterations):
		counts,centers = get_centers(k, disco.core.Disco(master).new_job(
			name = 'kmeans_iterate_'+str(i),
			input = input,
			map_reader = map_reader,
			map = estimate_map, 
			combiner = estimate_combiner,
			reduce = estimate_reduce,
			nr_reduces = nr_reduces, 
			params = disco.core.Params(centers=centers,dist=d2,update_center=update_center,get_center=get_center)))

		print 'cluster sizes after iteration %d:' %(i,), ' '.join("%d:%d" %(k,v) for k,v in counts.iteritems())

	return centers


def predict(input, centers, master="disco://discomaster", map_reader=disco.func.chain_reader, nr_reduces=None):
	if nr_reduces==None: nr_reduces=len(centers)

	results = disco.core.Disco(master).new_job(
		name = 'kmeans_output', 
		input = input, 
		map_reader = map_reader, 
		map = predict_map,
		nr_reduces = nr_reduces, 
		params = disco.core.Params(centers=centers,dist=d2),
		sort = False, clean = True).wait()

	return results
