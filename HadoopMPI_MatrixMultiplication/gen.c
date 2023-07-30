#include <stdio.h>
#include <stdlib.h>
#include <time.h>

void generate_random_matrices(char *filename1, char *filename2, int size)
{
    FILE *fp1 = fopen(filename1, "w");
    FILE *fp2 = fopen(filename2, "w");
    if (fp1 == NULL || fp2 == NULL)
    {
        printf("Error: could not open file\n");
        exit(1);
    }
    srand(time(NULL));
    for (int i = 0; i < size; i++)
    {
        for (int j = 0; j < size; j++)
        {
            fprintf(fp1, "%d ", rand() % 80 + 10);
            fprintf(fp2, "%d ", rand() % 80 + 10);
        }
        fprintf(fp1, "\n");
        fprintf(fp2, "\n");
    }
    fclose(fp1);
    fclose(fp2);
}
void mult(char *file1, char *file2, int size)
{
    FILE *fp1 = fopen(file1, "r");
    FILE *fp2 = fopen(file2, "r");
    if (fp1 == NULL || fp2 == NULL)
    {
        printf("Error: could not open file\n");
        exit(1);
    }
    int *matrix1 = (int *)malloc(size * size * sizeof(int));
    int *matrix2 = (int *)malloc(size * size * sizeof(int));
    int *matrix3 = (int *)malloc(size * size * sizeof(int));

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
            matrix3[i * size + j] = 0;
            for (int k = 0; k < size; k++)
            {
                matrix3[i * size + j] += matrix1[i * size + k] * matrix2[k * size + j];
            }
        }
    }
    FILE *fp3 = fopen("output_matrix.txt", "w");
    if (fp3 == NULL)
    {
        printf("Error: could not open file\n");
        exit(1);
    }
    for (int i = 0; i < size; i++)
    {
        for (int j = 0; j < size; j++)
        {
            fprintf(fp3, "%d ", matrix3[i * size + j]);
        }
        fprintf(fp3, "\n");
    }
    fclose(fp3);
}
int main(int argc, char **argv)
{
    // if (argc != 4)
    // {
    //     printf("Usage: %s <matrix1> <matrix2> <size>\n", argv[0]);
    //     exit(1);
    // }
    char filename1[] = "input_matrix1.txt";
    char filename2[] = "input_matrix2.txt";
    generate_random_matrices(filename1, filename2, atoi(argv[1]));
    mult(filename1, filename2, atoi(argv[1]));
    return 0;
}

/*
g++ gen.c -o gen && ./gen 16
*/