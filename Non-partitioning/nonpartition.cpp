#include <stdio.h>
#include "mpi.h"
#include <omp.h>
#include <string>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <iterator>
#include <omp.h>

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
//printf("\nPlease Enter input relations\n");
if(argc < 3)
{
	printf("\nPlease Enter input relations\n");
	return 0;
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

		//int i;
		//i = omp_get_dynamic(); 
		//printf("hello---->%d\n",i );
		//omp_set_dynamic(1); //set dynamic threads
		//i = omp_get_dynamic(); 
		//printf("byee---->%d\n",i );
		omp_set_num_threads(2); //set upper limit to 8

	/*_out *ot;
	int blocklengthso [] = {1, 1, 1};
	MPI_Datatype typelisto [] = {MPI_STRING, MPI_STRING, MPI_STRING};
	MPI_Aint addro[4], dispo[3];
	MPI_Address(ot, &addro[0]);
	MPI_Address(&(ot->key), &addro[1]);
	MPI_Address(&(ot->val1), &addro[2]);
	MPI_Address(&(ot->val2), &addro[3]);
	dispo[0] = addro[1] - addro[0];
	dispo[0] = 0;
	dispo[1] = addro[2] - addro[0];
	dispo[2] = addro[3] - addro[0];
	MPI_Datatype MPI_OUTRECORD;
	MPI_Type_struct(3, blocklengthso, dispo, typelisto, &MPI_OUTRECORD);
	MPI_Type_commit(&MPI_OUTRECORD);*/

	int size = 0, tag = 0;

	if(rank == 0) {
		vector<record> r;
		vector<record> s;
  		r = read_data(argv[1]);
		s = read_data(argv[2]);
		size = r.size() < s.size() ? r.size() : s.size();

		for (int i = 1; i < numprocs;i++)
		{
			MPI_Send(&size, 1, MPI_INT, i, tag, MPI_COMM_WORLD);
		}
		tag++;
		//printf("Size of smaller relation sent.\n");

		if(r.size() < s.size())
		{
			for (int i = 1; i < numprocs; i++)
				MPI_Send(&r.front(), size, MPI_RECORD, i, tag, MPI_COMM_WORLD);
		}
		else
		{
			for (int i = 1; i < numprocs; i++)
				MPI_Send(&s.front(), size, MPI_RECORD, i, tag, MPI_COMM_WORLD);
		}
		tag++; 
		//printf("Smaller relation sent.\n");

		int elem_per_proc[numprocs];
		int elem = r.size() > s.size() ? r.size() : s.size();
		#pragma omp parallel for
		for(int i = 0; i<numprocs; i++) {
			elem_per_proc[i] = elem/numprocs;	
		}
		#pragma omp parallel for
		for(int i = 0; i < elem%numprocs; i++) {
			elem_per_proc[i]++;	
		}
		for (int i = 1; i < numprocs; i++)
			MPI_Send(&elem_per_proc[i], 1, MPI_INT, i, tag, MPI_COMM_WORLD);
		tag++;
		//printf("Size of partition of larger relation sent.\n");

		int sent = elem_per_proc[0];

		if(r.size() > s.size())
		{
			for (int i = 1; i < numprocs; i++) 
			{
				MPI_Send(&r[sent], elem_per_proc[i], MPI_RECORD, i, tag, MPI_COMM_WORLD);
				sent += elem_per_proc[i];
			}
		}
		else {
			for (int i = 1; i < numprocs; i++) 
			{
				MPI_Send(&s[sent], elem_per_proc[i], MPI_RECORD, i, tag, MPI_COMM_WORLD);
				sent += elem_per_proc[i];
			}
		}
		tag++; 
		//printf("Larger relation sent.\n");
		
		
		unordered_map<string, string> hash_table;
		for(int i = 0; i < r.size(); i++) {
    			hash_table.insert({r[i].key, r[i].value});
		}

		vector<outrec> output_priv;
		_out out; 
		#pragma omp parallel for
		for(int i = 0; i < s.size(); i++) {
			unordered_map<string,string>::const_iterator got = hash_table.find(s[i].key);
    			if (got != hash_table.end()) {
				strcpy(out.key, s[i].key);
				strcpy(out.val1, got->second.c_str());
				strcpy(out.val2, s[i].value);
				#pragma omp critical
				output_priv.push_back(out);  
			}
		}	

		int recv[numprocs-1];
		#pragma omp parallel for
		for (int i = 0; i < numprocs - 1; i++)
			recv[i] = 0;
		elem = 0;

		for (int i = 1; i < numprocs; i++) {
			MPI_Status stat;
			MPI_Recv(&recv[i-1], 1, MPI_INT, i, tag, MPI_COMM_WORLD, &stat);
			elem += recv[i-1];
		}
		tag++;
		//cout << "No. of tuples in output " << elem << '\n';

		elem += output_priv.size();
		vector<outrec> output(elem);
		elem = output_priv.size();
		#pragma omp parallel for
		for(int i = 0; i < output_priv.size(); i++) {
    			output[i] = output_priv[i];
		}
		
		for (int i = 1; i < numprocs; i++) {
			MPI_Status stat;
			MPI_Recv(&output[elem], recv[i-1]*3, MPI_STRING, i, tag, MPI_COMM_WORLD, &stat);
			elem += recv[i-1];
		}
				
		FILE *fp; 
		fp = fopen("output.txt", "w");
		for(int i = 0; i != output.size(); i++) {
    			fprintf(fp, "%s\t\t%s\t\t%s\n", output[i].key, output[i].val1, output[i].val2); 
		}

		//cout << "We're done here!\n";		
	
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

		unordered_map<string, string> hash_table;
		for(int i = 0; i != r.size(); i++) {
    			hash_table.insert({r[i].key, r[i].value});
		}

		vector<outrec> output;
		_out out; 
		#pragma omp parallel for
		for(int i = 0; i < s.size(); i++) {
			unordered_map<string,string>::const_iterator got = hash_table.find(s[i].key);
    			if (got != hash_table.end()) {
				strcpy(out.key, s[i].key);
				strcpy(out.val1, got->second.c_str());
				strcpy(out.val2, s[i].value);
				#pragma omp critical 
				output.push_back(out);
				//printf("%s %s %s\n", s[i].key, got->second.c_str(), s[i].value);   
			}
		}	

		size = output.size();
		MPI_Send(&size, 1, MPI_INT, root, tag, MPI_COMM_WORLD);
		tag++;

		MPI_Send(&output.front(), size*3, MPI_STRING, root, tag, MPI_COMM_WORLD);	
		//cout << "Here in " << rank << '\n';
		
	}

	MPI_Finalize();

}



