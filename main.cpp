#include <iostream>
#include <vector>
#include <ctime>
#include <cstdlib>
#include <random>
#include <cmath>
#include <map>
#include <set>
#include <iomanip>
#include <fstream>
#include <queue>
#include <unistd.h>
#include <mpi.h>

using namespace std;

#define MAX_NODES 1400000

#define BEGIN_TAG 0
#define LIST_TAG 1

#define RSVP 2
#define ACCEPTED 3
#define DECLINED 4
#define DONE 5
#define BFS_TAG 6


random_device rd;
mt19937 gen(rd());
uniform_int_distribution<> gen_dis(0, numeric_limits<int>::max());


vector<vector<int> > adj(MAX_NODES);
int max_node = -1;

void init_adj_file(){

    cout << "Opening graph file..." << endl;

    fstream fin;

    char path[] = "graphs/CA-AstroPh.txt";
//    char path[] = "graphs/facebook_combined.txt";
//    char path[] = "graphs/roadNet-TX.txt";

    fin.open(path);

    if (!fin) {
        cerr << "Unable to open file " << path << endl;
        exit(1);   // call system to stop
    } else {
        cout << "Graph file {" << path << "} Opened" << endl;
    }

    int u, v;
    while (fin >> u >> v){
        adj[u].push_back(v);

        if (u > max_node) max_node = u + 1;
    }

    cout << "Adjacency List initiated." << endl;
    cout << "Number of nodes: " << max_node << endl;
}


vector<vector<int> > my_adj(MAX_NODES);
vector<int> my_nodes;
set<int> my_edges;

bool visit[MAX_NODES];
map<int, int> parent;
map<int, int> component;

int current_component = 0;


int main(int argc, char* argv[]) {
    MPI_Init(&argc, &argv);

    int rank;
    int size;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

//    cout << "Rank/Size:" << rank << "/" << size << endl;

    if (rank == 0) {
        cout << "Initiating graph!" << endl;
        init_adj_file();

        cout << rank << ":" << "Sending 'begin' message!" << endl;
        for (int i = 1; i < size; i++) {
            int buff[] = {max_node};
            MPI_Send(buff, 1, MPI_INT, i, BEGIN_TAG, MPI_COMM_WORLD);
        }

        // Sending Adjacency List
        for (int i = 0; i < max_node; i++) {
            if (i % size != 0) {
//                cout << rank << ":" << "Sending vertex " << i << " to node " << i%size << endl;

//                if (adj[i].empty())
//                    cerr << "Adjacency List of node {" << i << "} is empty" << endl;

                int *buff = new int[MAX_NODES];

                //First one is the node!
                buff[0] = i;
                //Second one is the length
                buff[1] = adj[i].size();

                for (unsigned int j = 0; j < adj[i].size(); j++) {
                    buff[j + 2] = adj[i][j];
                }

                MPI_Send(buff, adj[i].size() + 2, MPI_INT, i % size, LIST_TAG, MPI_COMM_WORLD);

                delete[] buff;
            }
        }
    }

    // Let us receive the list!
    if (rank == 0) { // Ranks 0 does not receive anything, but collects the data from the `adj`
        for (int i = 0; i < max_node; i ++) {
            if (i % size == 0){
                my_adj.push_back(adj[i]);
                my_nodes.push_back(i);

                for (int j = 0; j < adj[i].size(); j++)
                    my_edges.insert(adj[i][j]);
            }
        }
    } else {
        int buff[1];
        MPI_Status status;

        // Beginning
        MPI_Recv(buff, 1, MPI_INT, 0, BEGIN_TAG, MPI_COMM_WORLD, &status);

        max_node = buff[0];

        // Calculating number of nodes to receive:
        int rec_nodes = max_node / size;
        if (rank < max_node % size) rec_nodes++;

        cout << rank << ": " << "Total vertices: " << max_node << endl;
        cout << rank << ": " << "Expecting to receive " << rec_nodes << " vertices" << endl;

        // Receiving nodes

        for (int i = 0; i < rec_nodes; i ++) {
            int buff[MAX_NODES];
            MPI_Status status;
            MPI_Recv(buff, MAX_NODES, MPI_INT, 0, LIST_TAG, MPI_COMM_WORLD, &status);

            int u = buff[0];
            int len = buff[1];

            for (int j = 2; j < len + 2; j++) {
                my_adj[u].push_back(buff[j]);
                my_edges.insert(buff[j]);
            }
            my_nodes.push_back(u);

//            cout << rank << ":" << "Received adjacency list of vertex {" << u << "} with length {" << len << "}" << endl;

        }
    }

    if (rank == 0) {
        cout << rank << ": Initiating BFS" << endl;
        int buff[] = {0, RSVP, 0, current_component}; //RSVP: who am I, what am I saying, To whom, current_component
        MPI_Send(buff, 4, MPI_INT, 0, BFS_TAG, MPI_COMM_WORLD);
    }

    int buff[4];
    MPI_Status status;

    while () { //TODO Create a condition!
        MPI_Recv(buff, 4, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

        if (buff[1] == RSVP){
            if (parent.find(buff[2]) == parent.end()) { // Node is new and not visited!
                parent[buff[2]] = buff[0];
                component[buff[2]] = buff[3];

                for (int i = 0; i < my_adj[buff[2]].size(); i++) {
                    int send_buff[] = {buff[2], RSVP, my_adj[buff[2]][i], buff[3]};
                }
            }
        } else if (buff[1] == ACCEPTED || buff[1] == DECLINED) {
            visit[buff[0]] = true;
        }
    }

    MPI_Finalize();
    return 0;
}