import disco

def estimate_map(e, params):
	x=map(float,e[1].split(' '))
	y=x[params.y_id]
	del x[params.y_id]

	return [(e[0],(x,y))]


def estimate_combiner(k, v, vals, done, params):
	if vals=={}: 
		vals['x']=[0.0]*len(v[0])
		vals['x2']=[0.0]*len(v[0])
		vals['xy']=[0.0]*len(v[0])
		vals['y']=0.0
		vals['c']=0

	if done:
		return [(k, ' '. join(map(repr,vals['x'] + vals['x2'] + vals['xy'] + [ vals['y'], vals['c'] ])))]

	for i in range(len(v)):
		vals['x'][i]+=v[0][i]
		vals['x2'][i]+=v[0][i]*v[0][i]
		vals['xy'][i]+=v[0][i]*v[1]
	vals['y']+=v[1]
	vals['c']+=1



def predict_map(e, params):
	x=map(float,e[1].split(' '))
	return [(e[0],' '.join(map(repr,[params[i][0]+params[i][1]*x[i] for i in range(len(params))])))]


def estimate(input, y_id, host="disco://localhost", map_reader=disco.chain_reader):
	results = disco.job(host, name = 'naive_linear_regression_estimate',
			    input_files = input, 
			    map_reader = map_reader, 
			    fun_map = estimate_map,
			    combiner = estimate_combiner, 
			    params=disco.Params(y_id=y_id),
			    sort = False, clean = False)

	c=0
	y=0.0
	l=None
	x=None
	x2=None
	xy=None

	for key,value in disco.result_iterator(results):
		v=map(float,value.split(' '))
		
		if l==None:
			l=(len(v)-2)/3
			x=[0.0]*l
			x2=[0.0]*l
			xy=[0.0]*l

		c+=v[-1]
		y+=v[-2]
		for i in range(l):
			x[i]+=v[i]
			x2[i]+=v[l+i]
			xy[i]+=v[2*l+i]

	b = [ (c*xy[i] - x[i]*y)/(c*x2[i]+x[i]*x[i]) for i in range(l) ]
	a = [ (y-b[i]*x[i])/c for i in range(l) ]

	return zip(*(a,b))


def predict(input, model, host="disco://localhost", map_reader=disco.chain_reader):
	results = disco.job(host, name = 'naive_linear_regression_predict',
			    input_files = input, 
			    map_reader = map_reader, 
			    fun_map = predict_map,
			    params=model,
			    sort = False, clean = False)

	return results
