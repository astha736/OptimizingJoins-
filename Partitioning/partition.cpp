#include <stdio.h>
#include "mpi.h"
#include <omp.h>
#include <string>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <iterator>

#define LEN 25
#define root 0

using namespace std;

struct record {
char key[LEN]; 
char value[LEN];
};

struct outrec {
char key[LEN]; 
char val1[LEN];
char val2[LEN];
};

typedef struct record _record;
typedef struct outrec _out;

vector<record> read_data(char *file) {

	vector<record> r;
	_record rec;
	fstream fs;
	fs.open(file, fstream::in);
	if(fs.is_open()) {
   		while(fs.peek() != EOF) {
			fs.getline(rec.key, LEN, '\t');
			fs.getline(rec.value, LEN);
			r.push_back(rec);
   		}
   	fs.close();
	}
	return r;

}

int main(int argc, char *argv[]) {
	if(argc < 3) {
	return 1;
	}
	int numprocs, rank, namelen;
	char processor_name[MPI_MAX_PROCESSOR_NAME];
	int iam = 0, np = 1;
	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Get_processor_name(processor_name, &namelen);

	MPI_Datatype MPI_STRING;
	MPI_Type_contiguous(LEN, MPI_UNSIGNED_CHAR, &MPI_STRING);
	MPI_Type_commit(&MPI_STRING);

	_record *nt;
	int blocklengths [] = {1, 1};
	MPI_Datatype typelist [] = {MPI_STRING, MPI_STRING};
	MPI_Aint addr[3], disp[2];
	MPI_Address(nt, &addr[0]);
	MPI_Address(&(nt->key), &addr[1]);
	MPI_Address(&(nt->value), &addr[2]);
	disp[0] = addr[1] - addr[0];
	disp[1] = addr[2] - addr[0];
	MPI_Datatype MPI_RECORD;
	MPI_Type_struct(2, blocklengths, disp, typelist, &MPI_RECORD);
	MPI_Type_commit(&MPI_RECORD);

	int size = 0, tag = 0, flag;

	if(rank == 0) {

		vector<record> r;
		vector<record> s;
  		r = read_data(argv[1]);
		s = read_data(argv[2]);
		size = r.size() < s.size() ? r.size() : s.size();

		// Distribute tuples of smaller relation among nodes
		int elem_per_proc[numprocs-1];
		int elem = r.size() < s.size() ? r.size() : s.size();
		flag = r.size() < s.size() ? 0 : 1;
		#pragma omp parallel for
		for(int i = 0; i<(numprocs-1); i++) {
			elem_per_proc[i] = elem/(numprocs-1);	
		}
		#pragma omp parallel for
		for(int i = 0; i < elem%(numprocs-1); i++) {
			elem_per_proc[i]++;
			//cout << elem_per_proc[i] << '\n';	
		}
		
		for (int i = 1; i < numprocs; i++)
			MPI_Send(&elem_per_proc[i-1], 1, MPI_INT, i, tag, MPI_COMM_WORLD);
		tag++;

		int sent = 0;
		if(!flag)
			for (int i = 1; i < numprocs; i++) {
				MPI_Send(&r[sent], elem_per_proc[i-1], MPI_RECORD, i, tag, MPI_COMM_WORLD);
				sent += elem_per_proc[i-1];
			}
		else
			for (int i = 1; i < numprocs; i++) {
				MPI_Send(&s[sent], elem_per_proc[i-1], MPI_RECORD, i, tag, MPI_COMM_WORLD);
				sent += elem_per_proc[i-1];
			}
		tag++; 

		// Distribute tuples of larger relation among nodes
		elem = r.size() > s.size() ? r.size() : s.size();
		#pragma omp parallel for
		for(int i = 0; i<(numprocs-1); i++) {
			elem_per_proc[i] = elem/(numprocs-1);	
		}
		#pragma omp parallel for
		for(int i = 0; i < elem%(numprocs-1); i++) {
			elem_per_proc[i]++;	
			//cout << elem_per_proc[i] << '\n';
		}

		for (int i = 1; i < numprocs; i++)
			MPI_Send(&elem_per_proc[i-1], 1, MPI_INT, i, tag, MPI_COMM_WORLD);
		tag++;

		sent = 0;
		if(flag)
			for (int i = 1; i < numprocs; i++) {
				MPI_Send(&r[sent], elem_per_proc[i-1], MPI_RECORD, i, tag, MPI_COMM_WORLD);
				sent += elem_per_proc[i-1];
			}
		else
			for (int i = 1; i < numprocs; i++) {
				MPI_Send(&s[sent], elem_per_proc[i-1], MPI_RECORD, i, tag, MPI_COMM_WORLD);
				sent += elem_per_proc[i-1];
			}
		tag++;
		
		int recv[numprocs-1];		 
		#pragma omp parallel for
		for (int i = 0; i < numprocs - 1; i++)
			recv[i] = 0;

		for (int i = 1; i < numprocs; i++) {
			MPI_Status stat;
			MPI_Recv(&recv[i-1], 1, MPI_INT, i, tag, MPI_COMM_WORLD, &stat);
		}
		tag++;

		vector<vector<record>> partitions_r(numprocs-1);
		_out out;
		_record rec_out;
		int temp;
		for (int i = 1; i < numprocs; i++) {
			MPI_Status stat;
			temp = tag;
			for (int j = 0; j < recv[i-1]; j++) {
				MPI_Recv(&out, 3, MPI_STRING, i, tag, MPI_COMM_WORLD, &stat);
				tag++;
				strcpy(rec_out.key, out.key);
				strcpy(rec_out.value, out.val1);
				partitions_r[atol(out.val2)].push_back(rec_out);
			}
			tag = temp;
		}
		tag++;

		int recv_s[numprocs-1];
		#pragma omp parallel for
		for (int i = 0; i < numprocs - 1; i++)
			recv_s[i] = 0;
		
		for (int i = 1; i < numprocs; i++) {
			MPI_Status stat;
			MPI_Recv(&recv_s[i-1], 1, MPI_INT, i, 0, MPI_COMM_WORLD, &stat);
		}
		tag++;

		vector<vector<record>> partitions_s(numprocs-1);
		_out out_s;
		_record rec_out_s;
		tag = 0;
		int temp_s;
		for (int i = 1; i < numprocs; i++) {
			MPI_Status stat;
			temp_s = tag;
			for (int j = 0; j < recv[i-1]; j++) {
				MPI_Recv(&out_s, 3, MPI_STRING, i, tag, MPI_COMM_WORLD, &stat);
				tag++;
				strcpy(rec_out_s.key, out_s.key);
				strcpy(rec_out_s.value, out_s.val1);
				partitions_s[atol(out_s.val2)].push_back(rec_out_s);
			}
			tag = temp_s;
		}

		for (int i = 1; i < numprocs; i++) {
			int psize = partitions_r[i-1].size();
			MPI_Send(&psize, 1, MPI_INT, i, 100, MPI_COMM_WORLD);
		}

		for(int i = 1; i < numprocs; i++) {
			int count = partitions_r[i-1].size();
			MPI_Send(&partitions_r[i-1][0], count, MPI_RECORD, i, 200, MPI_COMM_WORLD);	
		}

		for (int i = 1; i < numprocs; i++) {
			int psize = partitions_s[i-1].size();
			MPI_Send(&psize, 1, MPI_INT, i, 300, MPI_COMM_WORLD);
		}

		for(int i = 1; i < numprocs; i++) {
			int count = partitions_s[i-1].size();
			MPI_Send(&partitions_s[i-1][0], count, MPI_RECORD, i, 400, MPI_COMM_WORLD);	
		}
		//cout << "HERE" << '\n';

		#pragma omp parallel for
		for (int i = 0; i < numprocs - 1; i++)
			recv[i] = 0;
		elem = 0;
		for (int i = 1; i < numprocs; i++) {
			MPI_Status stat;
			MPI_Recv(&recv[i-1], 1, MPI_INT, i, 500, MPI_COMM_WORLD, &stat);
			elem += recv[i-1];
		}
		tag++;
		//cout << "No. of tuples in output " << elem << '\n';

		vector<outrec> output(elem);
		elem = 0;

		for (int i = 1; i < numprocs; i++) {
			MPI_Status stat;
			MPI_Recv(&output[elem], recv[i-1]*3, MPI_STRING, i, 600, MPI_COMM_WORLD, &stat);
			elem += recv[i-1];
		}
				
		FILE *fp; 
		fp = fopen("output.txt", "w");
		for(int i = 0; i != output.size(); i++) {
    			fprintf(fp, "%s\t\t%s\t\t%s\n", output[i].key, output[i].val1, output[i].val2); 
		}
	}

	else {

		MPI_Status stat;
		MPI_Recv(&size, 1, MPI_INT, root, tag, MPI_COMM_WORLD, &stat);
		tag++;

		vector<record> r(size);

		MPI_Recv(&r.front(), size, MPI_RECORD, root, tag, MPI_COMM_WORLD, &stat);
		tag++;
	
		MPI_Recv(&size, 1, MPI_INT, root, tag, MPI_COMM_WORLD, &stat);
		tag++;

		vector<record> s(size);

		MPI_Recv(&s.front(), size, MPI_RECORD, root, tag, MPI_COMM_WORLD, &stat);
		tag++;
		cout << r.size() << " in process " << rank << '\n';
		cout << s.size() << " in process " << rank << '\n';
		
		unordered_map<string, string> hashmap;

 		unordered_map<string, string>::hasher fn = hashmap.hash_function();

		vector<outrec> outr;
		_out out; 

		#pragma omp parallel for
		for(int i = 0; i < r.size(); i++) {
    	
				strcpy(out.key, r[i].key);
				strcpy(out.val1, r[i].value);
				sprintf(out.val2, "%ld", fn(r[i].key)%(numprocs-1));
				#pragma omp critical
				outr.push_back(out);
				
		}	
		
		size = outr.size();
		MPI_Send(&size, 1, MPI_INT, root, tag, MPI_COMM_WORLD);
		tag++;
		
		for (int i = 0; i < size; i++) {
			MPI_Send(&outr[i], 3, MPI_STRING, root, tag, MPI_COMM_WORLD);
			tag++;	
		}
		
		vector<outrec> outs;
		_out out_s; 
		#pragma omp parallel for
		for(int i = 0; i < s.size(); i++) {
    	
				strcpy(out_s.key, s[i].key);
				strcpy(out_s.val1, s[i].value);
				sprintf(out_s.val2, "%ld", fn(s[i].key)%(numprocs-1));
				#pragma omp critical
				outs.push_back(out_s);
				
		}	

		size = outs.size();
		MPI_Send(&size, 1, MPI_INT, root, 0, MPI_COMM_WORLD);
		tag = 0;
		
		for (int i = 0; i < size; i++) {
			MPI_Send(&outs[i], 3, MPI_STRING, root, tag, MPI_COMM_WORLD);
			tag++;	
		}

		int count;
		MPI_Recv(&count, 1, MPI_INT, root, 100, MPI_COMM_WORLD, &stat);
	
		vector<record> partition_r(count);
		MPI_Recv(&partition_r[0], count, MPI_RECORD, root, 200, MPI_COMM_WORLD, &stat);

		MPI_Recv(&count, 1, MPI_INT, root, 300, MPI_COMM_WORLD, &stat);
	
		vector<record> partition_s(count);
		MPI_Recv(&partition_s[0], count, MPI_RECORD, root, 400, MPI_COMM_WORLD, &stat);

		unordered_map<string, string> hash_table;
		for(int i = 0; i < partition_r.size(); i++) {
    			hash_table.insert({partition_r[i].key, partition_r[i].value});
		}

		vector<outrec> output;
		#pragma omp parallel for
		for(int i = 0; i < partition_s.size(); i++) {
			unordered_map<string,string>::const_iterator got = hash_table.find(partition_s[i].key);
    			if (got != hash_table.end()) {
				strcpy(out.key, partition_s[i].key);
				strcpy(out.val1, got->second.c_str());
				strcpy(out.val2, partition_s[i].value);
				#pragma omp critical
				output.push_back(out);
				//printf("%s %s %s\n", s[i].key, got->second.c_str(), s[i].value);   
			}
		}	

		size = output.size();
		MPI_Send(&size, 1, MPI_INT, root, 500, MPI_COMM_WORLD);
		tag++;

		MPI_Send(&output.front(), size*3, MPI_STRING, root, 600, MPI_COMM_WORLD);	
		
	}
	
	MPI_Finalize();

}
