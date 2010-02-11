import disco

def init_map(e, params):
	import random
	return [(random.randint(0,params.k-1),e[1])]


def estimate_map(e, params):
	return[(min([(params.dist(c,map(float,e[1].split(' '))),i) for (i,c) in enumerate(params.centers)])[1],e[1])]


def estimate_combiner(k, v, centers, done, params):
    if done:
        return [(i,' '.join(map(repr,c))) for (i,c) in centers.iteritems()]
    else:
		v=map(float,v.split(' '))
        if not centers.has_key(k): centers[k]=[0.0]*len(v) + [0]
        for i in range(len(v)): centers[k][i]+=v[i]

		centers[k][len(v)]+=1


def estimate_reduce(iter, out, params):
    x={}
    for k,v in iter:
        y=map(float,v.split(' '))
        if not x.has_key(k): 
			x[k]=y
        else:
            for i in y: x[k][i]+=y[i]

    for k,v in x.iteritems():
        for i in range(len(v)-1): v[i]/=v[-1]
        out.add(k,' '.join(map(repr,v)))


def predict_map(e, params):
	return [(e[0],min([(params.dist(c,map(float,map(float,e[1].split(' ')))),i) for (i,c) in enumerate(params.centers)])[1])]


def d2(x,y): return sum([(x[i]-y[i])**2 for i in range(len(x))])


def estimate(input, centers, k, iterations=10, host="disco://localhost", map_reader=disco.chain_reader, nr_reduces=None):
	if centers!=None: k=len(centers)
	if nr_reduces==None: nr_reduces=k

	results=None
	if centers==None:	
		results = disco.job(host, name = 'kmeans_init',
				    input_files = input, 
				    map_reader = map_reader, 
				    fun_map = init_map, 
				    combiner = estimate_combiner, 
				    reduce = estimate_reduce,
				    nr_reduces = nr_reduces, 
				    params = disco.Params(k=k),
				    sort = False, clean = True)

	for i in range(iterations):
		if results!=None:
			centers=[None]*k
			counts=[None]*k
			for key,value in disco.result_iterator(results):
				x=map(float,value.split(' '))
				centers[int(key)]=x[:-1]
				counts[int(key)]=x[-1]

		results = disco.job(host, name = 'kmeans_iterate_'+str(i),
				    input_files = input,
				    map_reader = map_reader,
				    fun_map = estimate_map, 
				    combiner = estimate_combiner,
				    reduce = estimate_reduce,
				    nr_reduces = nr_reduces, 
				    params = disco.Params(centers=centers,dist=d2),
				    sort = False, clean = True)
		
	return centers


def predict(input, centers, host="disco://localhost", map_reader=disco.chain_reader, nr_reduces=None):
	if nr_reduces==None: nr_reduces=len(centers)

	results = disco.job(host, name = 'kmeans_output', 
			    input_files = input, 
			    map_reader = map_reader, 
			    fun_map = predict_map,
			    nr_reduces = nr_reduces, 
			    params = disco.Params(centers=centers,dist=d2),
			    sort = False, clean = True)

	return results
