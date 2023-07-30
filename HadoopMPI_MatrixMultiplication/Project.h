#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <time.h>
#include <string.h>
#include <stdbool.h>
#include <math.h>
#include <sys/stat.h>
#include <sys/types.h>

#define clr_master "\x1b[22m \x1b[32m "
#define clr_servant "\x1b[22m \x1b[33m"
#define clr_def "\033[0m \x1b[1m"

#ifndef MATRIX_SIZE
#define MATRIX_SIZE 16
#endif
struct Keyvalue
{
    char *key, *value;
};
struct Helper
{
    int ith_key, jth_key;
    char matrix;
    int index;
    int value;
    bool flag;
};
int *size_allocator(int arr_size, int num_mappers)
{
    int *arr = (int *)malloc(num_mappers * sizeof(int));
    for (int i = 0; i < num_mappers; i++)
    {
        arr[i] = 0;
    }
    int counter = 0;

    while (counter < arr_size)
    {
        for (int i = 0; i < num_mappers; i++)
        {
            if (counter < arr_size)
            {
                arr[i] += 1;
                counter++;
            }
            else
            {
                break;
            }
        }
    }
    return arr;
}

void cmpMatrix(char *filename1, char *filename2, int size)
{
    FILE *fp1 = fopen(filename1, "r");
    FILE *fp2 = fopen(filename2, "r");
    if (fp1 == NULL || fp2 == NULL)
    {
        printf("Error: could not open file\n");
        exit(1);
    }

    int *matrix1 = (int *)malloc(size * size * sizeof(int));
    int *matrix2 = (int *)malloc(size * size * sizeof(int));

    for (int i = 0; i < size; i++)
    {
        for (int j = 0; j < size; j++)
        {
            fscanf(fp1, "%d", &matrix1[i * size + j]);
            fscanf(fp2, "%d", &matrix2[i * size + j]);
        }
    }
    fclose(fp1);
    fclose(fp2);

    for (int i = 0; i < size; i++)
    {
        for (int j = 0; j < size; j++)
        {
            if (matrix1[i * size + j] != matrix2[i * size + j])
            {
                printf("index: %d, %d\n", i, j);
                printf("\x1b[35m"
                       "Matrix comparison function: False\n");
                exit(1);
            }
        }
    }
    printf("\x1b[35m"
           "Matrix comparison function: True\n");
}

void mapper(int *matrix1, int *matrix2, int start, int end, int rank)
{
    printf(clr_servant "[SERVANT %d]:" clr_def " Mapper started\n", rank);
    struct Keyvalue *keyvalue = (struct Keyvalue *)malloc((end - start) * MATRIX_SIZE * MATRIX_SIZE * MATRIX_SIZE * 2 * sizeof(struct Keyvalue));
    int z = 0;

    for (int i = 0; i < end - start; i++)
    {
        for (int j = 0; j < MATRIX_SIZE; j++)
        {
            for (int k = 0; k < MATRIX_SIZE; k++)
            {
                keyvalue[z].key = (char *)malloc(100 * sizeof(char));
                keyvalue[z].value = (char *)malloc(100 * sizeof(char));
                sprintf(keyvalue[z].key, "(%d,%d)", start + i, k);
                sprintf(keyvalue[z].value, "(A,%d,%d)", j, matrix1[i * MATRIX_SIZE + j]);
                z++;
            }
        }
    }

    for (int j = 0; j < end - start; j++)
    {
        for (int k = 0; k < MATRIX_SIZE; k++)
        {
            for (int i = 0; i < MATRIX_SIZE; i++)
            {

                keyvalue[z].key = (char *)malloc(100 * sizeof(char));
                keyvalue[z].value = (char *)malloc(100 * sizeof(char));
                sprintf(keyvalue[z].key, "(%d,%d)", i, k);
                sprintf(keyvalue[z].value, "(B,%d,%d)", j + start, matrix2[j * MATRIX_SIZE + k]);
                z++;
            }
        }
    }
    char fname[] = "mapper_output";
    struct stat st;
    if (stat(fname, &st) != 0)
    {
        // Folder does not exist, create it
        int status = mkdir(fname, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    }
    // write value to file
    char filename[100];
    sprintf(filename, "mapper_output/mapper%d.txt", rank);

    FILE *fp1 = fopen(filename, "w");
    for (int i = 0; i < z; i++)
    {
        fprintf(fp1, "%s %s\n", keyvalue[i].key, keyvalue[i].value);
    }
    fclose(fp1);

    char *key = (char *)malloc(z * MATRIX_SIZE * MATRIX_SIZE * sizeof(char));
    char *value = (char *)malloc(z * MATRIX_SIZE * MATRIX_SIZE * sizeof(char));
    strcpy(key, "");
    strcpy(value, "");
    for (int i = 0; i < z; i++)
    {
        strcat(key, keyvalue[i].key);
        strcat(value, keyvalue[i].value);

        strcat(value, "/");
        strcat(key, "/");
    }

    int pair_size[2] = {strlen(key) + 1, strlen(value) + 1};
    int falg = 1;
    printf(clr_servant "[SERVANT %d]:" clr_def "\x1b[31m"
                      "Process %d has completed task <Map>\n ",
           rank,
           rank);

    printf(clr_servant "[SERVANT %d]:" clr_def " Sending key-value pair to MASTER\n", rank);
    MPI_Send(&falg, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
    // send length of key and value to reducer
    MPI_Send(&pair_size[0], 2, MPI_INT, 0, 0, MPI_COMM_WORLD);
    // send key and value to reducer
    MPI_Send(&key[0], strlen(key) + 1, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
    MPI_Send(&value[0], strlen(value) + 1, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
}

void shuffler(char *key, char *value, struct Keyvalue *result_kv_pair)
{
    char *key1 = (char *)malloc((strlen(key) + 1) * sizeof(char));
    char *value1 = (char *)malloc((strlen(value) + 1) * sizeof(char));
    strcpy(key1, key);
    strcpy(value1, value);

    int counter = 1;
    char *token = strtok(key1, "/");
    while (token != NULL)
    {
        counter++;
        token = strtok(NULL, "/");
    }
    struct Keyvalue *keyvalue = (struct Keyvalue *)malloc(counter * sizeof(struct Keyvalue));

    token = strtok(key, "/");
    int z = 0;
    while (token != NULL)
    {

        keyvalue[z].key = (char *)malloc(100 * sizeof(char));
        strcpy(keyvalue[z].key, token);
        z++;
        token = strtok(NULL, "/");
    }
    token = strtok(value, "/");
    z = 0;
    while (token != NULL)
    {
        keyvalue[z].value = (char *)malloc(100 * sizeof(char));
        strcpy(keyvalue[z].value, token);
        z++;
        token = strtok(NULL, "/");
    }

    for (int i = 0; i < MATRIX_SIZE * MATRIX_SIZE; i++)
    {
        result_kv_pair[i].key = (char *)malloc(100 * sizeof(char));
        result_kv_pair[i].value = (char *)malloc(500 * sizeof(char));
        strcpy(result_kv_pair[i].key, "");
        strcpy(result_kv_pair[i].value, "");
    }

    int k = 0;
    for (int i = 0; i < counter - 1; i++)
    {
        if (strcmp(keyvalue[i].key, "") == 0)
        {
            continue;
        }

        else
        {
            strcpy(result_kv_pair[k].key, keyvalue[i].key);
            strcat(result_kv_pair[k].value, keyvalue[i].value);

            for (int j = 0; j < counter - 1; j++)
            {
                if (strcmp(keyvalue[i].key, keyvalue[j].key) == 0 && i != j && strcmp(keyvalue[i].key, "") != 0)
                {

                    strcat(result_kv_pair[k].value, keyvalue[j].value);
                    strcpy(keyvalue[j].key, "");
                }
            }
            k++;

            if (k == (MATRIX_SIZE * MATRIX_SIZE))
            {
                break;
            }
        }
    }
}

void reducer(char *key, char *value, int rank)
{
    printf(clr_servant "[SERVANT %d]:" clr_def " Reducer started\n", rank);
    char *key1 = (char *)malloc((strlen(key) + 1) * sizeof(char));
    char *value1 = (char *)malloc((strlen(value) + 1) * sizeof(char));
    strcpy(key1, key);
    strcpy(value1, value);

    int counter = 1;
    char *token = strtok(key1, "/");
    while (token != NULL)
    {
        counter++;
        token = strtok(NULL, "/");
    }
    struct Keyvalue *keyvalue = (struct Keyvalue *)malloc(counter * sizeof(struct Keyvalue));

    token = strtok(key, "/");
    int z = 0;
    while (token != NULL)
    {

        keyvalue[z].key = (char *)malloc(100 * sizeof(char));
        strcpy(keyvalue[z].key, token);
        z++;
        token = strtok(NULL, "/");
    }
    token = strtok(value, "/");
    z = 0;
    while (token != NULL)
    {
        keyvalue[z].value = (char *)malloc(pow(MATRIX_SIZE, 3) * sizeof(char));
        strcpy(keyvalue[z].value, token);
        z++;
        token = strtok(NULL, "/");
    }
    int num_elements = 0;
    struct Helper *data;
    for (int i = 0; i < counter - 1; i++)
    {
        num_elements = 0;
        for (int j = 0; j < strlen(keyvalue[i].value); j++)
        {
            if (keyvalue[i].value[j] == '(')
            {
                num_elements++;
            }
        }

        data = (struct Helper *)malloc(num_elements * sizeof(struct Helper));
        // printf("num_elements %d\n", num_elements);
        // printf("key %s\n", keyvalue[i].key);
        // printf("value %s\n", keyvalue[i].value);
        token = strtok(keyvalue[i].value, "() ,");
        int j = 0;
        while (token != NULL)
        {
            if (token[0] == 'A' || token[0] == 'B')
            {
                data[j].matrix = token[0];
                data[j].index = atoi(strtok(NULL, "() ,"));
                data[j].value = atoi(strtok(NULL, "() ,"));
                data[j].flag = false;
                j++;
            }
            token = strtok(NULL, "() ,");
        }

        for (int k = 0; k < num_elements; k++)
        {
            if (data[k].flag == false)
            {
                for (int l = k + 1; l < num_elements; l++)
                {
                    if (data[k].index == data[l].index && data[k].matrix != data[l].matrix)
                    {
                        data[k].value = data[k].value * data[l].value;
                        data[k].flag = false;
                        data[l].flag = true;
                    }
                }
            }
        }

        for (int k = 1; k < num_elements; k++)
        {
            if (data[k].flag == false)
            {
                data[0].value += data[k].value;
                data[k].flag = true;
            }
        }
        keyvalue[i].value = (char *)malloc(100 * sizeof(char));
        sprintf(keyvalue[i].value, "%d", data[0].value);
    }
    char fname[] = "reducer_output";
    struct stat st;
    if (stat(fname, &st) != 0)
    {
        // Folder does not exist, create it
        int status = mkdir(fname, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    }
    // write to file reducer1.txt
    char filename[100];
    sprintf(filename, "reducer_output/reducer%d.txt", rank);
    printf(clr_servant "[SERVANT %d]:" clr_def " Writing to file %s \n", rank, filename);

    FILE *fp1 = fopen(filename, "w");
    for (int i = 0; i < counter - 1; i++)
    {
        fprintf(fp1, "%s %s\n", keyvalue[i].key, keyvalue[i].value);
    }
    fclose(fp1);
    printf(clr_servant "[SERVANT %d]:" clr_def "\x1b[31m"
                      "Process %d has completed task <Reduce>\n ",
           rank,
           rank);
}
