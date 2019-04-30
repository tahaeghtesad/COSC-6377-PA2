#include <iostream>
#include <vector>
#include <unordered_map>
#include <mpi.h>
#include <string>
#include <fstream>

using namespace std;

inline int getMachine(int v, int size){
    return v%size;
}

unordered_map<int, vector<int> > loadFileinMa\ster(int rank, int size, string filename){
    unordered_map<int, vector<int> > nodes;
    if(rank==0){
        ifstream fin;
        fin.open(filename);
        if(!fin){
            cerr << "File failed to open" << endl;
            exit(1);
        }
        int u, v;
        while(fin >> u >> v){
            if(nodes.find(u) != nodes.end()){
                nodes[u].push_back(v);
            }else{
                nodes[u] = vector<int>{v};
            }
        }

        int um;
        vector<int> adj;
        for(unordered_map<int, vector<int> >::iterator it = nodes.begin(); it < nodes.end(); it++){
            u = it->first;
            adj = it -> second;
        }
    }
    return nodes;
}


int main(int argc, char** argv){
    MPI_Init(&argc, &argv);

    int rank;
    int size;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    string filename = "graphs/CA-AstroPh.txt";

    unordered_map<int, vector<int> > nodes= loadFile(rank, size, filename);

    MPI_Finalize();
    return 0;
}