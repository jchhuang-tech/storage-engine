/*
 * YASE: Yet Another Storage Engine
 *
 * CMPT 454 Database Systems II Course Project, Fall 2021
 *
 * Copyright (C) School of Computing Science, Simon Fraser University
 *
 * Not for distribution without prior approval.
 *
 * Test cases for BaseFile abstraction.
 */

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <Storage/basefile.h>

class BaseFileTests : public ::testing::Test {
 protected:
  std::string bfile_name;
  yase::BaseFile *bfile;

  void SetUp() override {
    bfile = nullptr;
    bfile_name = "test_base";
    system(("rm -rf " + bfile_name).c_str());
  }
  void TearDown() override {
    if (bfile) {
      delete bfile;
    }
    system(("rm -rf " + bfile_name).c_str());
  }

  void NewBaseFile() {
    bfile = new yase::BaseFile(bfile_name.c_str());
    ASSERT_TRUE(bfile);
  }
};

// Creation test to check empty file parameters
TEST_F(BaseFileTests, NewBaseFile) {
  NewBaseFile();
  ASSERT_TRUE(bfile);

  // File IDs start with 1
  ASSERT_EQ(bfile->GetId(), 1);
}

// The second creation test, file ID should be 2
TEST_F(BaseFileTests, ID) {
  NewBaseFile();
  ASSERT_EQ(bfile->GetId(), 2);
}

// Page count should be 0
TEST_F(BaseFileTests, InitPageCount) {
  NewBaseFile();
  ASSERT_EQ(bfile->GetPageCount(), 0);
}


// The file's raw initial size should be 0
TEST_F(BaseFileTests, InitSize) {
  NewBaseFile();
  struct stat s;
  stat(bfile_name.c_str(), &s);
  ASSERT_EQ(s.st_size, 0);
}

// Create, read and write some pages
void AccessFile(yase::BaseFile *bf, uint32_t npages) {
  void *p = malloc(PAGE_SIZE);
  ASSERT_NE(p, nullptr);
  for (uint32_t i = 0; i < npages; ++i) {
    yase::PageId pid = bf->CreatePage();
    ASSERT_TRUE(pid.IsValid());

    // Load the page, should succeed
    bool success = bf->LoadPage(pid, p);
    ASSERT_TRUE(success);

    // Write the page back
    for (uint32_t j = 0; j < PAGE_SIZE / sizeof(uint32_t); ++j) {
      ((uint32_t *)p)[j] = j;
    }
    bf->FlushPage(pid, p);

    // Load again and verify result
    success = bf->LoadPage(pid, p);
    ASSERT_TRUE(success);
    for (uint32_t j = 0; j < PAGE_SIZE / sizeof(uint32_t); ++j) {
      ASSERT_EQ(((uint32_t *)p)[j], j);
    }
  }
  free(p);
}

// Load a page from a new empty file, should fail
TEST_F(BaseFileTests, LoadEmpty) {
  NewBaseFile();

  void *p = malloc(PAGE_SIZE);
  ASSERT_NE(p, nullptr);
  bool success = bfile->LoadPage(yase::PageId(0), p);
  free(p);
  ASSERT_FALSE(success);
}

// Create a page in a new file
TEST_F(BaseFileTests, CreatePage) {
  NewBaseFile();
  yase::PageId pid = bfile->CreatePage();
  ASSERT_TRUE(pid.IsValid());
}

// Single-threaded test with a single file
TEST_F(BaseFileTests, SingleThreadAccess) {
  static const uint32_t kPages = 10;

  NewBaseFile();

  AccessFile(bfile, kPages);

  // Check raw file size
  struct stat s;
  stat(bfile_name.c_str(), &s);
  ASSERT_EQ(s.st_size, kPages * PAGE_SIZE);
}

// Multi-threaded test of BaseFile
TEST_F(BaseFileTests, MultiThreadAccess) {
  // Step 1: Create a base file

  // Step 2: Create 4 threads using std::thread; each thread should execute the
  // AccessFile function to write 10 pages to the created base file

  // Step 3: Wait for all threads to finish and recycle them if dynamic
  // allocation (e.g., malloc/new) is used

  // Step 4: Check raw file size: assert (using ASSERT_EQ) the file size to be
  // 10 * PAGE_SIZE * [number of threads] bytes.
}

int main(int argc, char **argv) {
  ::google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

