#include "Project.h"

void main(int argc, char *argv[])
{

    int flag = 0;
    int size;
    int rank;
    int *matrix1;
    int *matrix2;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Status status;
    int count;
    int num_mappers = size - 1;
    int num_reducers = size - 1;
    char *reducer_key;
    char *reducer_value;
    char hostname[256];
    FILE *fp1, *fp2;
    if (size < 2)
    {
        printf("Error: Number of processes should be greater than 1\n");
        exit(1);
    }
    if (rank == 0)
    {

        gethostname(hostname, sizeof(hostname));
        printf(clr_master "[MASTER]:" clr_def "\x1b[31m"
                          " process_id %d  running on %s\n",
               rank, hostname);
        // printf("MATRX_SIZE: %d\n", MATRIX_SIZE);
        matrix1 = (int *)malloc(MATRIX_SIZE * MATRIX_SIZE * sizeof(int));
        matrix2 = (int *)malloc(MATRIX_SIZE * MATRIX_SIZE * sizeof(int));

        char *filename1 = "input_matrix1.txt";
        char *filename2 = "input_matrix2.txt";
        char *filename3 = "output_matrix.txt";

        fp1 = fopen(filename1, "r");
        fp2 = fopen(filename2, "r");
        if (fp1 == NULL || fp2 == NULL)
        {
            printf(clr_master "[MASTER]:" clr_def " Error--could not open matrix initiating files\n");
            exit(1);
        }

        for (int i = 0; i < MATRIX_SIZE; i++)
        {
            for (int j = 0; j < MATRIX_SIZE; j++)
            {
                fscanf(fp1, "%d", &matrix1[i * MATRIX_SIZE + j]);
                fscanf(fp2, "%d", &matrix2[i * MATRIX_SIZE + j]);
            }
        }
        fclose(fp1);
        fclose(fp2);
        printf(clr_master "[MASTER]:" clr_def " Matrix successfully populated\n");

        int *mapper_chunk = size_allocator(MATRIX_SIZE, num_mappers);
        int mapper_start_rows = 0, mapper_end_rows = 0;

        for (int i = 1; i <= num_mappers; i++)
        {
            printf(clr_master "[MASTER]:" clr_def "\x1b[31m"
                              " Task <Map> assigned to process %d\n",
                   i);

            if (mapper_chunk[i - 1] == 0)
            {
                MPI_Send(&mapper_chunk[i - 1], 1, MPI_INT, i, 0, MPI_COMM_WORLD);
                MPI_Send(&mapper_chunk[i - 1], 1, MPI_INT, i, 0, MPI_COMM_WORLD);
            }
            else
            {
                MPI_Send(&mapper_start_rows, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
                mapper_end_rows += mapper_chunk[i - 1];
                MPI_Send(&mapper_end_rows, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
                MPI_Send(&matrix1[mapper_start_rows * MATRIX_SIZE], mapper_chunk[i - 1] * MATRIX_SIZE, MPI_INT, i, 0, MPI_COMM_WORLD);
                MPI_Send(&matrix2[mapper_start_rows * MATRIX_SIZE], mapper_chunk[i - 1] * MATRIX_SIZE, MPI_INT, i, 0, MPI_COMM_WORLD);
                mapper_start_rows = mapper_end_rows;
            }
        }
    }

    if (rank != 0)
    {
        int start = 0, end = 0;
        char hostname[256];
        gethostname(hostname, sizeof(hostname));

        MPI_Recv(&start, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Recv(&end, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        printf(clr_servant "[SERVANT %d]:" clr_def "\x1b[31m"
                           " process_id %d  running on %s\n",
               rank, rank, hostname);

        matrix1 = (int *)malloc((end - start) * MATRIX_SIZE * sizeof(int));
        matrix2 = (int *)malloc((end - start) * MATRIX_SIZE * sizeof(int));

        if (start == 0 && end == 0)
        {
            printf(clr_servant "[SERVANT %d]:" clr_def " No work to do\n", rank);
            flag = -1;
            MPI_Send(&flag, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
        }
        else
        {

            MPI_Recv(&matrix1[0], (end - start) * MATRIX_SIZE, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Recv(&matrix2[0], (end - start) * MATRIX_SIZE, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            gethostname(hostname, sizeof(hostname));
            printf(clr_servant "[SERVANT %d]:" clr_def "\x1b[31m"
                               "Process %d received Task <Map> on %s\n",
                   rank, rank, hostname);

            mapper(matrix1, matrix2, start, end, rank);
        }
    }

    if (rank == 0)
    {

        int total_key_size = 0, total_value_size = 0, pair_size[2];
        int process_flag = 0;

        printf(clr_master "[MASTER]:" clr_def " Waiting for SERVANTs to finish\n");
        for (int i = 1; i <= num_mappers; i++)
        {
            MPI_Recv(&process_flag, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            if (process_flag == -1)
            {
                printf(clr_master "[MASTER]:" clr_def " No word done by SERVANT %d\n", i);
                num_mappers--;
            }
            else if (process_flag == 0)
            {
                printf(clr_master "[MASTER]:" clr_def " SERVANT %d mapper failed\n", i);
                exit(1);
            }
            else if (process_flag == 1)
            {
                printf(clr_master "[MASTER]:" clr_def " SERVANT %d done\n", i);
            }
        }

        for (int i = 1; i <= num_mappers; i++)
        {
            MPI_Recv(&pair_size[0], 2, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            total_key_size += pair_size[0];
            total_value_size += pair_size[1];
        }

        char *key = (char *)malloc(total_key_size * sizeof(char));
        char *value = (char *)malloc(total_value_size * sizeof(char));
        strcpy(key, "");
        strcpy(value, "");
        int temp_key_size = 0, temp_value_size = 0;
        char *temp_key;
        char *temp_value;
        for (int i = 1; i <= num_mappers; i++)
        {
            MPI_Probe(i, 0, MPI_COMM_WORLD, &status);
            MPI_Get_count(&status, MPI_CHAR, &count);

            temp_key = (char *)malloc(count * sizeof(char));
            MPI_Recv(&temp_key[0], count, MPI_CHAR, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            total_key_size += count;
            strcat(key, temp_key);

            MPI_Probe(i, 0, MPI_COMM_WORLD, &status);
            MPI_Get_count(&status, MPI_CHAR, &count);
            temp_value = (char *)malloc(count * sizeof(char));

            MPI_Recv(&temp_value[0], count, MPI_CHAR, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            total_value_size += count;
            strcat(value, temp_value);
        }

        struct Keyvalue *result_kv_pair = (struct Keyvalue *)malloc((MATRIX_SIZE * MATRIX_SIZE * sizeof(struct Keyvalue)));
        printf(clr_master "[MASTER]:" clr_def " Shuffling data\n");
        shuffler(key, value, result_kv_pair);

        int *reducer_chunk = size_allocator(MATRIX_SIZE * MATRIX_SIZE, num_reducers);

        int next_size = reducer_chunk[0];

        for (int i = 1, j = 0; i <= num_reducers; i++)
        {
            printf(clr_master "[MASTER]:" clr_def "\x1b[31m"
                              " Task <Reduce> assigned to process %d\n",
                   i);

            reducer_key = (char *)malloc(reducer_chunk[i - 1] * 8 * MATRIX_SIZE * sizeof(char));
            reducer_value = (char *)malloc(reducer_chunk[i - 1] * 15 * pow(MATRIX_SIZE, 2) * sizeof(char));
            strcpy(reducer_key, "");
            strcpy(reducer_value, "");

            for (; j < next_size; j++)
            {
                strcat(reducer_key, result_kv_pair[j].key);
                strcat(reducer_value, result_kv_pair[j].value);
                strcat(reducer_key, "/");
                strcat(reducer_value, "/");
            }
            printf(clr_master "[MASTER]:" clr_def " Sending data to REDUCER %d\n", i);
            MPI_Send(&reducer_chunk[i - 1], 1, MPI_INT, i, 0, MPI_COMM_WORLD);
            MPI_Send(&reducer_key[0], strlen(reducer_key) + 1, MPI_CHAR, i, 0, MPI_COMM_WORLD);
            MPI_Send(&reducer_value[0], strlen(reducer_value) + 1, MPI_CHAR, i, 0, MPI_COMM_WORLD);

            if (i != num_reducers)
            {
                next_size += reducer_chunk[i];
            }

            strcpy(reducer_key, "");
            strcpy(reducer_value, "");
        }
    }

    if (rank != 0)
    {
        int reducer_chunk_size = 0;
        printf(clr_servant "[SERVANT %d]:" clr_def " Waiting for data from MASTER\n", rank);
        MPI_Recv(&reducer_chunk_size, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        gethostname(hostname, sizeof(hostname));
        printf(clr_servant "[SERVANT %d]:" clr_def "\x1b[31m"
                           "Process %d received Task <Map> on %s\n",
               rank, rank, hostname);
        MPI_Probe(0, 0, MPI_COMM_WORLD, &status);
        MPI_Get_count(&status, MPI_CHAR, &count);
        reducer_key = (char *)malloc(count * sizeof(char));
        MPI_Recv(&reducer_key[0], count, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        MPI_Probe(0, 0, MPI_COMM_WORLD, &status);
        MPI_Get_count(&status, MPI_CHAR, &count);
        reducer_value = (char *)malloc(count * sizeof(char));
        MPI_Recv(&reducer_value[0], count, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        printf(clr_servant "[SERVANT %d]:" clr_def " Data received from MASTER\n", rank);

        reducer(reducer_key, reducer_value, rank);

        flag = 1;
        MPI_Send(&flag, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
    }

    if (rank == 0)
    {
        matrix1 = (int *)malloc(MATRIX_SIZE * MATRIX_SIZE * sizeof(int));
        int x, y, value;

        flag = 0;
        for (int i = 1; i <= num_reducers; i++)
        {
            MPI_Recv(&flag, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            if (flag == 0)
            {
                printf(clr_master "[MASTER]:" clr_def " REDUCER %d failed\n", i);
                exit(0);
            }
            else
            {
                printf(clr_master "[MASTER]:" clr_def " Data received from REDUCER %d\n", i);
                char filename[100];
                int str_size = (8 + (MATRIX_SIZE * 5));
                char line[size];

                printf(clr_master "[MASTER]:" clr_def " Reading data from SERVANT %d's file\n", i);
                sprintf(filename, "reducer_output/reducer%d.txt", i);

                fp1 = fopen(filename, "r");
                if (fp1 == NULL)
                {
                    printf(clr_master "[MASTER]:" clr_def " Error--Could not open file %s\n", filename);
                    exit(0);
                }
                while (fgets(line, str_size, fp1) != NULL)
                {
                    sscanf(line, "(%d,%d) %d", &x, &y, &value);
                    matrix1[x * MATRIX_SIZE + y] = value;
                }

                fclose(fp1);
                printf(clr_master "[MASTER]:" clr_def " Data read from SERVANT %d's file\n", i);
            }
        }

        fp1 = fopen("result.txt", "w");
        if (fp1 == NULL)
        {
            printf(clr_master "[MASTER]:" clr_def " Error--Could not open file result.txt\n");
            exit(0);
        }
        for (int i = 0; i < MATRIX_SIZE; i++)
        {
            for (int j = 0; j < MATRIX_SIZE; j++)
            {
                fprintf(fp1, "%d ", matrix1[i * MATRIX_SIZE + j]);
            }
            fprintf(fp1, "\n");
        }
        fclose(fp1);

        printf(clr_master "[MASTER]:" clr_def " Result written to result.txt\n");

        char filename1[] = "output_matrix.txt";
        char filename2[] = "result.txt";
        cmpMatrix(filename1, filename2, MATRIX_SIZE);
    }

    MPI_Finalize();
}