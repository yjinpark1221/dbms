#include "file.h"

std::map<int, table_t> fd2table;
std::map<table_t, int> table2fd;
std::vector<int> openedFds;

table_t next_table_id() {
    static table_t table_id = 1;
    return table_id++;
}

// Open existing database file or create one if it doesn't exist
// int file_open_database_file (const char * pathname)
// • Open the database file.
// • It opens an existing database file using ‘pathname’ or create a new file if absent.
// • If a new file needs to be created, the default file size should be 10 MiB.
// • Then it returns the file descriptor of the opened database file.
// • All other 5 commands below should be handled after open data file.
table_t file_open_table_file(const char* pathname) {
    std::string path = pathname;
    path = path.substr(4); // remove DATA
    int fd = open(pathname, O_RDWR);
    if (fd < 0) {
        fd = open(pathname, O_RDWR | O_CREAT, 0644);
        if (fd < 0) {
            perror("in file_open_database_file negative fd");
            return fd;
        } // controlled in higher layer
        pagenum_t* buf = (pagenum_t*)malloc(PAGE_SIZE);
        memset(buf, 0, PAGE_SIZE);
        buf[0] = 1; // free page
        buf[1] = 2560; // num page
        buf[2] = 0; // root page
        if (pwrite(fd, buf, PAGE_SIZE, 0) <= 0) {
            perror("in file_open_database_file pwrite error");
            exit(0);
        }
        sync();
        for (pagenum_t i = 1; i < 2560; ++i) {
            buf[0] = (i + 1) % 2560;
            if (pwrite(fd, buf, PAGE_SIZE, PAGE_SIZE * i) <= 0) {
                perror("in file_open_database_file pwrite error");
                exit(0);
            }
        }
        sync();
        free(buf);
    }
    table_t table_id = std::stoi(path);
    fd2table[fd] = table_id;
    table2fd[table_id] = fd;

    openedFds.push_back(fd);
    return table_id;
}

// Allocate an on-disk page from the free page list
// uint64_t file_alloc_page (int fd);
// • Allocate a page.
// • It returns a new page # from the free page list.
// • If the free page list is empty, then it should grow the database file and return a free page #.
pagenum_t file_alloc_page(table_t table_id) {
    int fd = table2fd[table_id];
    page_t page;
    if (pread(fd, &page, PAGE_SIZE, 0) <= 0) {
        perror("file_alloc_page pread error");
        exit(0);
    }
    pagenum_t freePage = ((pagenum_t*)page.a)[0], numPage = ((pagenum_t*)page.a)[1], rootPage = ((pagenum_t*)page.a)[2];
    if (freePage == 0) {
        for (pagenum_t i = 0; i < numPage; ++i) {
            ((pagenum_t*)page.a)[0] = (numPage + i + 1) % (2 * numPage);
            if (pwrite(fd, &page, PAGE_SIZE, (numPage + i) * PAGE_SIZE) <= 0) {
                perror("file_alloc_page pwrite error");
                exit(0);
            }
            sync();
        }// add free pages : numPage ~ (2 * numPage - 1)
        freePage = numPage;
        numPage *= 2;
    }
    if (pread(fd, &page, PAGE_SIZE, freePage * PAGE_SIZE) <= 0) {
        perror("file_alloc_page pread error");
        exit(0);
    }//get next free page : ((pagenum_t*)page.a)[0]
    ((pagenum_t*)page.a)[1] = numPage;
    ((pagenum_t*)page.a)[2] = rootPage;
    if (pwrite(fd, &page, PAGE_SIZE, 0) <= 0) {
        perror("file_alloc_page pread error");
        exit(0);
    }
    sync();//change the first free page
    return freePage;
}

// Free an on-disk page to the free page list
// void file_free_page (int fd, uint64_t page_number);
// • Free a page.
// • It informs the disk space manager of returning the page with ‘page_number’ for freeing it to the free page list.
void file_free_page(table_t table_id, pagenum_t pagenum) {
    int fd = table2fd[table_id];
    pagenum_t* buf = (pagenum_t*)malloc(PAGE_SIZE);
    memset(buf, 0, PAGE_SIZE);
    if (pread(fd, buf, PAGE_SIZE, 0) <= 0) {
        perror("file_free_page pread error");
        exit(0);
    }
    pagenum_t freePage = buf[0];            // read original free page
    buf[0] = pagenum;
    if (pwrite(fd, buf, PAGE_SIZE, 0) <= 0) {
        perror("file_free_page pwrite error");
        exit(0);
    }
    sync();// change the first free page
    buf[0] = freePage;
    if (pwrite(fd, buf, PAGE_SIZE, pagenum * PAGE_SIZE) <= 0) {
        perror("file_free_page pwrite error");
        exit(0);
    }
    sync();// link to original free page
    free(buf);
    return;
}

// Read an on-disk page into the in-memory page structure(dest)
// file_read_page <page_number, dest> - read page
// • It fetches the disk page corresponding to ‘page_number’ to the in-memory buffer (i.e., ‘dest’).
void file_read_page(table_t table_id, pagenum_t pagenum, page_t* dest) {
    int fd = table2fd[table_id];
    if (dest == NULL) {
        perror("file_read_page dest NULL");
        exit(0);
    }
    if (pread(fd, dest, PAGE_SIZE, pagenum * PAGE_SIZE) <= 0) {
        perror("file_read_page pread error");
        exit(0);
    }
}

// Write an in-memory page(src) to the on-disk page
// void file_write_page (int fd, uint64_t page_number, const char * src);
// • Write a page.
// • It writes the in-memory page content in the buffer (i.e., ‘src’) to the disk page pointed by ‘page_number’
void file_write_page(table_t table_id, pagenum_t pagenum, const page_t* src) {
    int fd = table2fd[table_id];
    if (src == NULL) {
        perror("file_write_page src NULL");
        exit(0);
    }
    // printf("fd %d, src %p, pagenum %d\n", fd, src, pagenum);
    if (pwrite(fd, src, PAGE_SIZE, pagenum * PAGE_SIZE) <= 0) {
        perror("file_write_page pwrite error");
        exit(0);
    }
    sync();
}

// Close the database file
// void file_close_database_file();
// • Close the database file.
// • This API doesn’t receive a file descriptor as a parameter. So a means for referencing the descriptor of the opened file(i.e., global variable) is required.
void file_close_table_file(){
    for (int fd : openedFds) {
        if (fd > 0 && close(fd) < 0) {
            perror("file_close_database_file close error");
            exit(0);
        }
    }
    fd2table.clear();
    table2fd.clear();
    openedFds.clear();
}

/// below are the APIs for testing ///

// gets file size from the header page
pagenum_t file_get_size(table_t table_id) {
    int fd = table2fd[table_id];
    pagenum_t buf;
    pread(fd, &buf, sizeof(pagenum_t), sizeof(pagenum_t));
    return buf;
}

// checks if a page number is in the vector(free page list)
bool inVec(std::vector<pagenum_t> vec, pagenum_t page) {
    for (pagenum_t p : vec) {
        if (page == p) return 1;
    }
    return 0;
}

// gets the free page list as a vector
std::vector<pagenum_t> file_get_free_list(table_t table_id) {
    int fd = table2fd[table_id];
    pagenum_t pagenum = lseek(fd, 0, SEEK_END) / PAGE_SIZE;
    std::vector<pagenum_t> vec;
    pagenum_t* buf = (pagenum_t*) malloc(PAGE_SIZE * pagenum);
    memset(buf, 0, PAGE_SIZE);
    pread(fd, buf, PAGE_SIZE * pagenum, 0);
    pagenum_t freePage = buf[0];
    for (; freePage; freePage = buf[PAGE_SIZE / 8 * freePage]) {
        vec.push_back(freePage);
    }
    free(buf);
    return vec;
}
