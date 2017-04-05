Compile:
mpic++ -fopenmp -o nonp nonpartition.cpp -std=c++11

Run executable:
mpirun -np <noOfProcesses> <InputRelationA> <InputRelationB>
mpirun -np 4 ./nonp ../Data/pos.txt ../Data/emp.txt

Format of input files:
<key>\t<value>

