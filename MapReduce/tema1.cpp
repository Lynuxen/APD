#include <math.h>
#include <pthread.h>

#include <algorithm>
#include <charconv>
#include <cmath>
#include <fstream>
#include <iostream>
#include <set>
#include <string>
#include <vector>

const int ARBITRARY_LIMIT = 2048;

struct mapper {
    bool did_work;
    std::vector<std::vector<int>> numbers;
};

struct arguments {
    int id;
    int nmb_mappers;
    int nmb_reducers;
    int *nmb_cur_files;
    pthread_barrier_t *barrier;
    pthread_mutex_t *mutex;
    std::vector<std::string> *files;
    struct mapper cur_mapper;
    std::vector<struct mapper> *mappers;
};

int binarySearch(int arr[], int l, int r, int number, int power) {
    if (r >= l) {
        int mid = l + (r - l) / 2;

        if (pow(arr[mid], power) == number) {
            return mid;
        }

        if (pow(arr[mid], power) > number) {
            return binarySearch(arr, l, mid - 1, number, power);
        }

        return binarySearch(arr, mid + 1, r, number, power);
    }
    return -1;
}

bool perfectPower(int arr[], int number, int power) {
    if (power == 2) {
        int r = sqrt(number);

        if (number > 0 && r * r == number) {
            return true;
        }
    } else {
        auto res = binarySearch(arr, 0, ARBITRARY_LIMIT - 1, number, power);
        if (res > 0) {
            return true;
        }
    }

    return false;
}

void *map_reduce(void *args) {
    struct arguments *data = (struct arguments *)args;

    if (data->id < data->nmb_mappers) {
        int possible[ARBITRARY_LIMIT];
        for (auto i = 1; i < ARBITRARY_LIMIT; i++) {
            possible[i] = i;
        }

        std::ifstream input_file;
        std::string file_name;
        std::string buffer;
        struct mapper cur_mapper;
        std::vector<int> placeholder;
        for (auto i = 0; i < data->nmb_reducers; i++) {
            cur_mapper.numbers.push_back(placeholder);
        }
        cur_mapper.did_work = false;

        while (true) {
            pthread_mutex_lock(data->mutex);

            if (data->files->size() == 0) {
                pthread_mutex_unlock(data->mutex);
                break;
            }

            file_name = data->files->back();
            data->files->pop_back();
            pthread_mutex_unlock(data->mutex);

            input_file.open(file_name);
            if (!input_file.is_open()) {
                std::cerr << "mapper cant open file " << file_name << std::endl;
                exit(-1);
            }

            getline(input_file, buffer);
            while (getline(input_file, buffer)) {
                int number;

                std::from_chars(buffer.data(),
                                buffer.data() + buffer.size(), number);

                for (auto i = 2; i < data->nmb_reducers + 2; i++) {
                    int res = perfectPower(possible, number, i);
                    if (res) {
                        cur_mapper.numbers[i - 2].push_back(number);
                    }
                }
            }

            cur_mapper.did_work = true;
            input_file.close();
        }
        if (cur_mapper.did_work) {
            pthread_mutex_lock(data->mutex);
            data->mappers->push_back(cur_mapper);
            pthread_mutex_unlock(data->mutex);
        }
    }

    pthread_barrier_wait(data->barrier);

    if (data->id >= data->nmb_mappers) {
        std::set<int> reduced;

        for (size_t i = 0; i < data->mappers->size(); i++) {
            for (auto number : data->mappers->at(i).numbers.at(data->id - data->nmb_mappers)) {
                reduced.insert(number);
            }
        }
        std::string file_out_name = "out" + std::to_string(data->id - data->nmb_mappers + 2) + ".txt";
        std::ofstream file_out;
        file_out.open(file_out_name);
        if (file_out.is_open()) {
            file_out << reduced.size();
        }
        file_out.close();
    }

    return NULL;
}

int main(int argc, char const *argv[]) {
    if (argc < 4) {
        std::cerr << "Usage:\n\t./tema1 nmb_mappers nmb_reducers input_file" << std::endl;
        exit(-1);
    }

    int res;
    void *status;

    int nmb_files, nmb_mappers, nmb_reducers, nmb_threads;
    int count = 0;

    pthread_t *threads;
    pthread_barrier_t barrier;
    pthread_mutex_t mutex;

    struct arguments *args;

    nmb_mappers = atoi(argv[1]);
    nmb_reducers = atoi(argv[2]);
    nmb_threads = nmb_mappers + nmb_reducers;

    std::vector<struct mapper> mappers;

    args = new struct arguments[nmb_threads];

    std::string buffer;
    std::ifstream test_file;

    std::vector<std::string> files;

    test_file.open(argv[3]);
    if (test_file.is_open()) {
        while (getline(test_file, buffer)) {
            if (count == 0) {
                std::from_chars(buffer.data(),
                                buffer.data() + buffer.size(), nmb_files);
                count++;
            } else {
                files.push_back(buffer);
            }
        }
    }

    test_file.close();

    threads = new pthread_t[nmb_threads];

    pthread_barrier_init(&barrier, NULL, nmb_threads);
    pthread_mutex_init(&mutex, NULL);

    for (auto i = 0; i < nmb_threads; i++) {
        args[i].id = i;
        args[i].nmb_mappers = nmb_mappers;
        args[i].nmb_reducers = nmb_reducers;
        args[i].nmb_cur_files = &nmb_files;
        args[i].barrier = &barrier;
        args[i].mutex = &mutex;
        args[i].files = &files;
        args[i].mappers = &mappers;

        res = pthread_create(&threads[i], NULL, map_reduce, &args[i]);
        if (res) {
            std::cerr << "Eroare la crearea thread-ului %d" << std::endl;
        }
    }

    for (auto i = 0; i < nmb_threads; i++) {
        res = pthread_join(threads[i], &status);

        if (res) {
            std::cerr << "Eroare la asteptarea thread-ului %d" << std::endl;
            exit(-1);
        }
    }

    return 0;
}