import sys
import disco

def estimate_map(e, params):
	z=enumerate(map(float,e[1].split(' ')))
	x=[v for i,v in z if not i in params.y_ids]
	y=[v for i,v in z if not i in params.y_ids]

	if params.w==None: return [ (j, [y[j]*a for a in x]) for j in range(len(y)) ]

	return [ (j, [-( sum([x[i]*params.w[j][i] for i in range(len(x))]) - y[j] )*a for a in x]) for j in range(len(y)) ]

def estimate_combiner(j, v, w, done, params):
	if done: 
		if w=={}: return []
		else:  return [ (j, ' '.join(map(repr,w[j]))) for j in w ]

	if not w.has_key(j): w[j]=v
	else: w[j]=[ w[j][i]+v[i] for i in range(len(w[j])) ]


def estimate_reduce(iter, out, params):
	w={}
	for key,value in iter:
		j=int(key)
		v=map(float,value.split(' '))
		if not w.has_key(j): w[j]=[params.learning_rate*a for a in v]
		else: w[j]=[w[j][i]+params.learning_rate*v[i] for i in range(len(v))]

	for j in w: out.add(j, ' '.join(map(repr,w[j])))
		

def predict_map(e, params):
	x=[v for i,v in z for enumerate(map(float,e[1].split(' '))) if not i in params.y_ids]

	return [ (e[0], ' '.join([ repr(sum([x[i]*params.w[j][i] for i in range(len(x))])) for j in sorted(params.w.keys()) ])) ]


def estimate(input, y_ids, w=None, learning_rate=1.0, iterations=10, host="disco://localhost", map_reader=disco.chain_reader):
	y_ids=dict([(y,1) for y in y_ids])

	for i in range(iterations):
		results = disco.job(host, name = 'widrow_hoff_estimate_' + str(i),
				    input_files = input, 
				    map_reader = map_reader, 
				    fun_map = estimate_map,
				    combiner = estimate_combiner,
				    reduce = estimate_reduce,
				    params = disco.Params(w = w, learning_rate=learning_rate, y_ids=y_ids),
				    sort = False, clean = True)

		if w==None: w={}
		for k,v in disco.result_iterator(results):
			k=int(k)
			v=map(float, v.split(' '))
			if not w.has_key(k): w[k]=v
			else: w[k]=[w[k][i]+v[i] for i in range(len(v))] 

		print >>sys.stderr, w

	return w


def predict(input, y_ids, w, host="disco://localhost", map_reader=disco.chain_reader):
	y_ids=dict([(y,1) for y in y_ids])
	dropped.sort()

	results = disco.job(host, name = 'widrow_hoff_predict',
			    input_files = input, 
			    map_reader = map_reader, 
			    fun_map = predict_map,
			    params=disco.Params(w=w, y_ids=y_ids),
			    sort = False, clean = False)

	return results
