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
#include <unordered_map>
#include <unistd.h>
#include <mpi.h>

using namespace std;

#define MAX_NODES 2000000

#define BEGIN_TAG 0
#define LIST_TAG 1
#define NOMINATE_TAG 7
#define NOMINATE_TAG_RESP 8
#define ADJ_DONE 9
#define FINISH_TAG 10

#define RSVP 2
#define ACCEPTED 3
#define DECLINED 4
#define DONE 5
#define BFS_TAG 6
#define BFS_MESSAGE_LENGTH 4


#define cout if(false)cout


random_device rd;
mt19937 gen(rd());
uniform_int_distribution<> gen_dis(0, numeric_limits<int>::max());

int max_node = -1;
unordered_map<int, vector<int> > adj;

void init_adj_file(char *path) {

    cout << "Opening graph file..." << endl;

    fstream fin;

//    char path[] = "graphs/CA-AstroPh.txt";
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
    while (fin >> u >> v) {
        adj[u].push_back(v);
        if (u > max_node) max_node = u + 1;
    }

//    cout << "Adjacency List initiated." << endl;
//    cout << "Number of nodes: " << adj.size() << endl;
}


unordered_map<int, vector<int> > my_adj;
set<int> my_nodes;

map<int, int> parent;
map<int, int> component;

unordered_map<int, set<int> > explore_set;

int current_component = 0;

int root = 0;

int main(int argc, char *argv[]) {
    MPI_Init(&argc, &argv);

    int rank;
    int size;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    cout << "Rank/Size:" << rank << "/" << size << endl;

    if (rank == 0) {
//        cout << "Initiating graph!" << endl;
        init_adj_file(argv[1]);

        // Sending Adjacency List
        for (unordered_map<int, vector<int> >::iterator it = adj.begin(); it != adj.end(); it++) {
            int u = it->first;
            if (u % size != 0) {
//                cout << rank << ":" << "Sending vertex " << i << " to node " << i%size << endl;
//
//                if (adj[i].empty())
//                    cerr << "Adjacency List of node {" << i << "} is empty" << endl;

                int *buff = new int[adj[u].size() + 5];

                //First one is the node!
                buff[0] = u;

                //Second one is the length
                buff[1] = adj[u].size();

                for (unsigned int j = 0; j < adj[u].size(); j++) {
                    buff[j + 2] = it->second[j];
                }

                MPI_Send(buff, adj[u].size() + 2, MPI_INT, it->first % size, LIST_TAG, MPI_COMM_WORLD);

                delete[] buff;
            }
        }

        for (int i = 1; i < size; i++) {
            int buff[] = {0};
            MPI_Send(buff, 1, MPI_INT, i, ADJ_DONE, MPI_COMM_WORLD);
        }
    }

    // Let us receive the list!
    if (rank == 0) { // Ranks 0 does not receive anything, but collects the data from the `adj`
        for (unordered_map<int, vector<int> >::iterator it = adj.begin(); it != adj.end(); it++) {
            int u = it->first;
            if (u % size == 0) {
                my_adj[u] = adj[u];
                my_nodes.insert(u);
            }
        }
    } else {
        MPI_Status status;

        do {
            int* buff = new int[550];
            MPI_Recv(buff, MAX_NODES, MPI_INT, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

            int u = buff[0];
            int len = buff[1];

            if (buff[0] == 0) {
                continue;
                cout << buff[0] << " " << buff[1] << " " << buff[2] << " " << buff[3] << " " << buff[4] << " "
                     << buff[5] << endl;
            }

            for (int j = 2; j < len + 2; j++) {
                my_adj[u].push_back(buff[j]);
            }
            my_nodes.insert(u);

            delete[] buff;

        } while (status.MPI_TAG != ADJ_DONE);

    }
    // load is over

    int nominated = -1;
    int received = 0;

    if (rank == 0) {
        cout << "Nominating..." << endl;
        for (int i = 0; i < size; i++) {
            int buff[] = {0};
            MPI_Send(buff, 1, MPI_INT, i, NOMINATE_TAG, MPI_COMM_WORLD);
        }

        received = 0;
        nominated = -1;
    }

    while (true) {
        int buff[4];
        MPI_Status status;
        MPI_Recv(buff, BFS_MESSAGE_LENGTH, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

        cout << rank << ": " << "Src: " << status.MPI_SOURCE << " /Tag:" << status.MPI_TAG << " /Payload: " << buff[0]
             << endl;

        if (status.MPI_TAG == FINISH_TAG){
            break;

        } else if (status.MPI_TAG == NOMINATE_TAG) {

            if (my_nodes.begin() == my_nodes.end()) {
                buff[0] = -1;
            } else buff[0] = *my_nodes.begin();

            cout << rank << ": Nominating: " << buff[0] << endl;

            MPI_Send(buff, 1, MPI_INT, 0, NOMINATE_TAG_RESP, MPI_COMM_WORLD);

        } else if (status.MPI_TAG == NOMINATE_TAG_RESP) {

            received++;

            if (buff[0] != -1) {
                nominated = buff[0];
            }

            cout << "received: " << received << endl;

            if (received == size) {

                if (nominated == -1) {

                    cout << rank <<  ": Finishing the algorithm... " << endl;
                    for (int i = 0; i < size; i++) {
                        MPI_Send(&nominated, 1, MPI_INT, i, FINISH_TAG, MPI_COMM_WORLD);
                    }

                } else {

                    cout << "Finalizing {" << nominated << "} as BFS root" << endl;

                    int buff[] = {-size, RSVP, nominated,
                                  current_component}; //RSVP: who am I, what am I saying, To whom, current_component
                    MPI_Send(buff, 4, MPI_INT, nominated % size, BFS_TAG, MPI_COMM_WORLD);

                }
            }
        } else if (status.MPI_TAG == BFS_TAG) {

            cout << rank << ": " << "From: " << buff[0] << " /To: " << buff[2] << " /MSG: " << buff[1] << " /CMP: "
                 << buff[3] << endl;

            if (buff[1] == RSVP) {

                //            if (my_adj[buff[2]].empty()) {
                //                cout << rank << ": " << "{" << buff[2] << "} is not a node!" << endl;
                //                int send_buff[] = {buff[2], DECLINED, buff[0], current_component};
                //                MPI_Send(send_buff, BFS_MESSAGE_LENGTH, MPI_INT, buff[0] % size, BFS_TAG, MPI_COMM_WORLD);
                //            } else

                if (parent.find(buff[2]) == parent.end()) {
                    cout << rank << ": " << "{" << buff[2]
                         << "} is a new node and was not previously visited. Accepting RSVP." << endl;
                    parent[buff[2]] = buff[0];
                    component[buff[2]] = buff[3];

                    for (int i = 0; i < my_adj[buff[2]].size(); i++) {
                        int send_buff[] = {buff[2], RSVP, my_adj[buff[2]][i], buff[3]};
                        MPI_Send(send_buff, BFS_MESSAGE_LENGTH, MPI_INT, my_adj[buff[2]][i] % size, BFS_TAG,
                                 MPI_COMM_WORLD);
                        explore_set[buff[2]].insert(my_adj[buff[2]][i]);
                        //                    cout << rank << ": " << "{" << buff[2] << "} is RSVPing {" << my_adj[buff[2]][i] << "}" << endl;
                    }

                    int send_buff[] = {buff[2], ACCEPTED, buff[0], current_component};
                    MPI_Send(send_buff, BFS_MESSAGE_LENGTH, MPI_INT, buff[0] % size, BFS_TAG, MPI_COMM_WORLD);

                } else {
                    cout << rank << ": " << "{" << buff[2] << "} was previously visited. Declining RSVP." << endl;
                    int send_buff[] = {buff[2], DECLINED, buff[0], current_component};
                    MPI_Send(send_buff, BFS_MESSAGE_LENGTH, MPI_INT, buff[0] % size, BFS_TAG, MPI_COMM_WORLD);
                }

            } else if (buff[1] == ACCEPTED) {
                cout << rank << ": " << "{" << buff[0] << "} Accepted the RSVP. Should Update children of {" << buff[2]
                     << "} but it's not necessary." << endl;
            } else if (buff[1] == DECLINED) {
                cout << rank << ": " << "{" << buff[0] << "} Declined RSVP." << endl;
                explore_set[buff[2]].erase(buff[0]);
                if (explore_set[buff[2]].empty()) {
                    cout << rank << ": " << "{" << buff[2]
                         << "} has already visited all of its children. Sending done to parent." << endl;
                    int send_buff[] = {buff[2], DONE, parent[buff[2]], -1};
                    MPI_Send(send_buff, BFS_MESSAGE_LENGTH, MPI_INT, parent[buff[2]] % size, BFS_TAG,
                             MPI_COMM_WORLD);

                    my_nodes.erase(buff[2]);
                }

            } else if (buff[1] == DONE) {
                if (buff[2] == -size) {
                    cout << rank << ": " << "{" << buff[0]
                         << "} was a root and now, is done. We are going for the next root in a connected component."
                         << endl;


                    current_component++;
                    for (int i = 0; i < size; i++) {
                        int buff[] = {0};
                        MPI_Send(buff, 1, MPI_INT, i, NOMINATE_TAG, MPI_COMM_WORLD);

                        received = 0;
                        nominated = -1;
                    }

                    cout << rank << ": " << "Current Component: " << current_component << endl;

                } else {
                    explore_set[buff[2]].erase(buff[0]);
                    if (explore_set[buff[2]].empty()) {
                        cout << rank << ": " << "{" << buff[2]
                             << "} has already visited all of its children. Sending done to parent." << endl;
                        int send_buff[] = {buff[2], DONE, parent[buff[2]], -1};
                        MPI_Send(send_buff, BFS_MESSAGE_LENGTH, MPI_INT, parent[buff[2]] % size, BFS_TAG,
                                 MPI_COMM_WORLD);

                        my_nodes.erase(buff[2]);
                    }
                }
            }
        }
    }


    if (rank == 0) {
        cerr << rank << ": " << "The algorithm is finished. Number of Connected Components: " << current_component - 1
             << endl;
    }

    MPI_Finalize();
    return 0;
}