import disco

def estimate_map(e, params):
	z=dict([(elem,1) for elem in e[1].split(params.splitter)]).keys()
	x=[a for a in z if not a in params.ys]
	y=[a for a in z if a in params.ys]

	return [(b+params.splitter+a,1) for a in x for b in y] + [(a,1) for a in z] + [('',1)]
#[(b+params.splitter,1) for b in y] + [(params.splitter+a,1) for a in x] + [(params.splitter,1)]


def estimate_combiner(k, v, counts, done, params):
	if done: return counts.items()
	
	if not counts.has_key(k): counts[k]=v
	else: counts[k]+=v


def estimate_reduce(iter, out, params):
	counts={}

	for k,v in iter:
		v=int(v)
		if not counts.has_key(k): counts[k]=v
		else: counts[k]+=v

	for k,v in counts.iteritems(): out.add(k,repr(v))


def predict_map(e, params):
	ll=dict([(k,params.loglikelihoods[k]) for k in params.ys.keys()])

	for elem in e[1].split(params.splitter):
		if params.ys.has_key(elem): continue
		
		for y in params.ys:
			ll[y]+=params.loglikelihoods[y+params.splitter+elem]

	return [(e[0], k + ' ' + repr(ll[k])) for k in params.ys]


def estimate(input, ys, splitter=' ', host="disco://localhost", map_reader=disco.chain_reader):
	ys=dict([(id,1) for id in ys])

	results = disco.job(host, name = 'naive_bayes_estimate',
			    input_files = input, 
			    map_reader = map_reader, 
			    fun_map = estimate_map,
			    combiner = estimate_combiner,
			    reduce = estimate_reduce,
			    params = disco.Params(ys=ys,splitter=splitter),
			    sort = False, clean = False)

	total=0
	items={}
	classes={}
	pairs={}
	for key,value in disco.result_iterator(results):
		l=key.split(splitter)
		value=int(value)
		if len(l)==1: 
			if l[0]=='': total=value
			elif ys.has_key(l[0]): classes[l[0]]=value
			else: items[l[0]]=value
		else:
			pairs[key]=value

#counts[key]=[[c,i], [not c, i], [c, not i], [not c, not i]]
	counts={}
	for i in items:
		for y in ys:
			key=y+splitter+i
			counts[key]=[0,0,0,0]
			if pairs.has_key(key): counts[key][0]=pairs[key]
			counts[key][1]=items[i]-counts[key][0]
			if not classes.has_key(y): counts[key][2]=0
			else: counts[key][2]=classes[y]-counts[key][0]
			counts[key][3]=total-sum(counts[key][:3])

			# add pseudocounts
			counts[key]=map(lambda x: x+1, counts[key])
	total+=4

	import math
	loglikelihoods={}
	for key,value in counts.iteritems():
		log_c=math.log(value[0]+value[2])
		l=key.split(splitter)
		if not loglikelihoods.has_key(l[0]): loglikelihoods[l[0]]=0.0
		loglikelihoods[l[0]]+=math.log(value[2])-log_c
		loglikelihoods[key]=math.log(value[0])-math.log(value[2])

	return loglikelihoods


def predict(input, loglikelihoods, ys, splitter, host="disco://localhost", map_reader=disco.chain_reader):
	ys=dict([(id,1) for id in ys])
	results = disco.job(host, name = 'naive_bayes_predict',
			    input_files = input, 
			    map_reader = map_reader, 
			    fun_map = predict_map,
			    params=disco.Params(loglikelihoods=loglikelihoods,ys=ys,splitter=splitter),
			    sort = False, clean = False)

	return results
