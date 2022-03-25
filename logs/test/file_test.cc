#include <gtest/gtest.h>
#include "yjinbpt.h"
#include <string>

/*******************************************************************************
 * The test structures stated here were written to give you and idea of what a
 * test should contain and look like. Feel free to change the code and add new
 * tests of your own. The more concrete your tests are, the easier it'd be to
 * detect bugs in the future projects.
 ******************************************************************************/

/*
 * Tests file open/close APIs.
 * 1. Open a file and check the descriptor
 * 2. Check if the file's initial size is 10 MiB
 */
TEST(FileTest, HandlesInitialization) {
  // int fd;                                 // file descriptor
  // const char* pathname = "init_test";  // customize it to your test file

  // // Open a database file
  // fd = file_open_table_file(pathname);
  // // Check if the file is opened
  // ASSERT_TRUE(fd > 0);  // change the condition to your design's behavior

  // // Check the size of the initial file
  // int num_pages = file_get_size(fd);
  // EXPECT_EQ(num_pages, INITIAL_DB_FILE_SIZE / PAGE_SIZE)
  //     << "The initial number of pages does not match the requirement: "
  //     << num_pages;

  // // Close all database files
  // file_close_table_file();

  // // Remove the db file

  // ASSERT_EQ(remove(pathname), 0);
}

/*
 * Tests page allocation and free
 * 1. Allocate 2 pages and free one of them, traverse the free page list
 *    and check the existence/absence of the freed/allocated page
 */
TEST(FileTest, HandlesPageAllocation) {
  pagenum_t allocated_page, freed_page;
  int fd = file_open_table_file("allocFreeTest");
  // Allocate the pages
  allocated_page = file_alloc_page(fd);
  freed_page = file_alloc_page(fd);

  // Free one page
  file_free_page(fd, freed_page);

  std::vector<pagenum_t> v = file_get_free_list(fd);
  ASSERT_TRUE(inVec(v, freed_page));
  ASSERT_FALSE(inVec(v, allocated_page));
  remove("allocFreeTest");

}

TEST(FileTest, CheckReadWriteOperation) {
  int fd = file_open_table_file("readWriteTest");

  // Write
  char* src = (char*)malloc(PAGE_SIZE);
  char* dest = (char*)malloc(PAGE_SIZE);
  for (pagenum_t i = 0; i < PAGE_SIZE; ++i) {
    src[i] = 'a';
  }
  pagenum_t pagenum = file_alloc_page(fd);
  file_write_page(fd, pagenum, (page_t*)src);
  
  // Read
  file_read_page(fd, pagenum, (page_t*)dest);
  for (pagenum_t i = 0; i < PAGE_SIZE; ++i) {
    EXPECT_EQ(src[i], dest[i]);
  }
  free(src);
  free(dest);
  file_close_table_file();
  remove("readWriteTest");
}