# ParallelMatrixMultiplication-Hadoop-Map-Reduce
The code implements a distributed matrix multiplication algorithm using MPI. Here are the instructions to execute the code:

1. Compile the code using the mpicc compiler: mpicc Project.c -o main
2. Generate the input matrices if not present using the gen.c . The gen executable generates two input matrices of size MATRIX_SIZE x MATRIX_SIZE and also generates multiplication matrix in file output_matrix.txt. The size of the matrices is passed as a command-line argument. For example, to generate matrices of size 16, run the following command: g++ -o gen gen.c and then ./gen 16
3. Run the executable with mpirun. The number of processes to be used for the execution is passed as a command-line argument. For example, 4. To run the executable with 4 processes, run the following command: mpirun -np 4 ./main
After the execution is complete, comparison results will be present in the end and the result matrix can be found in the result.txt file.
