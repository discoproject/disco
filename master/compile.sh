if [ $NATIVE ]; then
	echo "Compile with Hipe"
	erlc +native +"{hipe, [o3]}" -o ebin src/*.erl
else
	echo "No Hipe"
	erlc -o ebin src/*.erl
fi
erl -pa ebin -noshell -run make_boot write_scripts
