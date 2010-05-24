import sys
import disco

def estimate_map(e, params):
	x=map(float,e[1].split(' '))
	y=x[params.y_id]
	del x[params.y_id]
	if params.w!=None and y*sum([x[i]*params.w[i] for i in range(len(params.w))])>0: return []
	return [('',[y*a for a in x])]


def estimate_combiner(k, v, w, done, params):
	if done: 
		if w=={}: return []
		else: return [('', ' '.join(map(repr,w[''])))]

	if w=={}: w['']=v
	else: w['']=[w[''][i]+v[i] for i in range(len(v))]


def estimate_reduce(iter, out, params):
    w=None
    for key,value in iter:
        v=map(float,value.split(' '))
        if w==None: w=[params.learning_rate*a for a in v]
        else: w=[w[i]+params.learning_rate*v[i] for i in range(len(v))]

	if w!=None: out.add('', ' '.join(map(repr,w)))


def predict_map(e, params):
	x=map(float,e[1].split(' '))
	del x[params.y_id]
	return [(e[0],sum([x[i]*params.w[i] for i in range(len(params.w))]))]


def estimate(input, y_id, w=None, learning_rate=1.0, iterations=10, host="disco://localhost", map_reader=disco.chain_reader):
	for i in range(iterations):
		results = disco.job(host, name = 'perceptron_estimate_' + str(i),
				    input_files = input, 
				    map_reader = map_reader, 
				    fun_map = estimate_map,
				    combiner = estimate_combiner,
				    reduce = estimate_reduce,
				    params = disco.Params(w = w, learning_rate=learning_rate,y_id=y_id),
				    sort = False, clean = True)

		for key,value in disco.result_iterator(results):
			v=map(float,value.split(' '))
			if w==None: w=v
			else: w=[w[i]+v[i] for i in range(len(w))]

		print >>sys.stderr,w

	return w


def predict(input, y_id, w, host="disco://localhost", map_reader=disco.chain_reader):
	results = disco.job(host, name = 'perceptron_predict',
			    input_files = input, 
			    map_reader = map_reader, 
			    fun_map = predict_map,
			    params=disco.Params(w=w, y_id=y_id),
			    sort = False, clean = False)

	return results
