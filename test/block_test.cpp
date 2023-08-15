// /*
// * Copyright Alibaba Group Holding Ltd.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
// #include <gtest/gtest.h>
// #include <iostream>
//
// #include "vec/columns/IColumn.h"
// #include "vec/columns/ColumnFactory.h"
// #include "vec/blocks/block.h"
// #include "vec/blocks/mutable_block.h"
//
// namespace LindormContest::test {
//
// using namespace vectorized;
//
// TEST(BlockTest, BlockTest) {
//     std::shared_ptr<ColumnString> col0 = std::dynamic_pointer_cast<ColumnString>(ColumnFactory::instance().create_column(ColumnType::COLUMN_TYPE_STRING, "col0"));
//     std::shared_ptr<ColumnInt32> col1 = std::dynamic_pointer_cast<ColumnInt32>(ColumnFactory::instance().create_column(ColumnType::COLUMN_TYPE_INTEGER, "col1"));
//     std::shared_ptr<ColumnInt64> col2 = std::dynamic_pointer_cast<ColumnInt64>(ColumnFactory::instance().create_column(ColumnType::COLUMN_TYPE_TIMESTAMP, "col2"));
//     std::shared_ptr<ColumnFloat64> col3 = std::dynamic_pointer_cast<ColumnFloat64>(ColumnFactory::instance().create_column(ColumnType::COLUMN_TYPE_DOUBLE_FLOAT, "col3"));
//     std::shared_ptr<ColumnFloat64> col4 = std::dynamic_pointer_cast<ColumnFloat64>(ColumnFactory::instance().create_column(ColumnType::COLUMN_TYPE_DOUBLE_FLOAT, "col4"));
//     std::shared_ptr<ColumnString> col5 = std::dynamic_pointer_cast<ColumnString>(ColumnFactory::instance().create_column(ColumnType::COLUMN_TYPE_STRING, "col5"));
//     Block block;
//     block.insert({col0, col0->get_type(), col0->get_name()});
//     block.insert({col1, col1->get_type(), col1->get_name()});
//     block.insert({col2, col2->get_type(), col2->get_name()});
//     block.insert({col3, col3->get_type(), col3->get_name()});
//     block.insert({col4, col4->get_type(), col4->get_name()});
//     block.insert({col5, col5->get_type(), col5->get_name()});
//     ASSERT_EQ(6, block.columns());
//     block.erase(0);
//     ASSERT_EQ(5, block.columns());
//     block.erase("col1");
//     ASSERT_EQ(4, block.columns());
//     std::set<size_t> positions {0, 1};
//     block.erase(positions);
//     ASSERT_EQ(2, block.columns());
//     ColumnWithTypeAndName& column = block.get_by_position(0);
//     ASSERT_EQ(ColumnType::COLUMN_TYPE_DOUBLE_FLOAT, column._type);
//     ASSERT_EQ("col4", column._name);
//     Block block_without_columns = block.clone_without_columns();
//     ASSERT_EQ(2, block_without_columns.columns());
//     ASSERT_EQ(nullptr, block_without_columns.get_by_position(0)._column);
//     ASSERT_EQ(ColumnType::COLUMN_TYPE_DOUBLE_FLOAT, block_without_columns.get_by_position(0)._type);
//     ASSERT_EQ("col4", block_without_columns.get_by_position(0)._name);
//     ASSERT_EQ(nullptr, block_without_columns.get_by_position(1)._column);
//     ASSERT_EQ(ColumnType::COLUMN_TYPE_STRING, block_without_columns.get_by_position(1)._type);
//     ASSERT_EQ("col5", block_without_columns.get_by_position(1)._name);
//     Block copy_block = block.copy_block();
//     ASSERT_EQ(2, copy_block.columns());
//     ASSERT_EQ(block.get_by_position(0)._column, copy_block.get_by_position(0)._column);
//     ASSERT_EQ(ColumnType::COLUMN_TYPE_DOUBLE_FLOAT, copy_block.get_by_position(0)._type);
//     ASSERT_EQ("col4", copy_block.get_by_position(0)._name);
//     ASSERT_EQ(block.get_by_position(1)._column, copy_block.get_by_position(1)._column);
//     ASSERT_EQ(ColumnType::COLUMN_TYPE_STRING, copy_block.get_by_position(1)._type);
//     ASSERT_EQ("col5", copy_block.get_by_position(1)._name);
// }
//
// TEST(BlockTest, MutableBlockTest) {
//     std::shared_ptr<ColumnString> col0 = std::dynamic_pointer_cast<ColumnString>(ColumnFactory::instance().create_column(ColumnType::COLUMN_TYPE_STRING, "col0"));
//     std::shared_ptr<ColumnInt32> col1 = std::dynamic_pointer_cast<ColumnInt32>(ColumnFactory::instance().create_column(ColumnType::COLUMN_TYPE_INTEGER, "col1"));
//     std::shared_ptr<ColumnInt64> col2 = std::dynamic_pointer_cast<ColumnInt64>(ColumnFactory::instance().create_column(ColumnType::COLUMN_TYPE_TIMESTAMP, "col2"));
//     std::shared_ptr<ColumnFloat64> col3 = std::dynamic_pointer_cast<ColumnFloat64>(ColumnFactory::instance().create_column(ColumnType::COLUMN_TYPE_DOUBLE_FLOAT, "col3"));
//     std::shared_ptr<ColumnFloat64> col4 = std::dynamic_pointer_cast<ColumnFloat64>(ColumnFactory::instance().create_column(ColumnType::COLUMN_TYPE_DOUBLE_FLOAT, "col4"));
//     std::shared_ptr<ColumnString> col5 = std::dynamic_pointer_cast<ColumnString>(ColumnFactory::instance().create_column(ColumnType::COLUMN_TYPE_STRING, "col5"));
//     col0->push_string("0");
//     col0->push_string("00");
//     col1->push_number(1);
//     col1->push_number(10);
//     col2->push_number(2);
//     col2->push_number(20);
//     col3->push_number(3.1);
//     col3->push_number(30.1);
//     col4->push_number(4.1);
//     col4->push_number(40.1);
//     col5->push_string("5");
//     col5->push_string("55");
//     Block block;
//     block.insert({col0, col0->get_type(), col0->get_name()});
//     block.insert({col1, col1->get_type(), col1->get_name()});
//     block.insert({col2, col2->get_type(), col2->get_name()});
//     block.insert({col3, col3->get_type(), col3->get_name()});
//     block.insert({col4, col4->get_type(), col4->get_name()});
//     block.insert({col5, col5->get_type(), col5->get_name()});
//     ASSERT_EQ(2, block.rows());
//     MutableBlock mutable_block = MutableBlock::build_mutable_block(&block);
//     ASSERT_EQ(6, mutable_block.columns());
//     ASSERT_EQ(2, mutable_block.rows());
//     mutable_block.append_block(&block, 0, block.rows());
//     ASSERT_EQ(4, mutable_block.rows());
//     Block new_block = mutable_block.to_block();
//     ASSERT_EQ(4, new_block.rows());
//     ASSERT_EQ(4, block.rows());
//     const ColumnString& new_col0 = reinterpret_cast<const ColumnString&>(*(new_block.get_by_position(0)._column));
//     ASSERT_EQ(4, new_col0.size());
//     ASSERT_EQ("0", new_col0[0]);
//     ASSERT_EQ("00", new_col0[1]);
//     ASSERT_EQ("0", new_col0[2]);
//     ASSERT_EQ("00", new_col0[3]);
//     const ColumnInt32& new_col1 = reinterpret_cast<const ColumnInt32&>(*(new_block.get_by_position(1)._column));
//     ASSERT_EQ(4, new_col1.size());
//     ASSERT_EQ(1, new_col1[0]);
//     ASSERT_EQ(10, new_col1[1]);
//     ASSERT_EQ(1, new_col1[2]);
//     ASSERT_EQ(10, new_col1[3]);
//     const ColumnInt64& new_col2 = reinterpret_cast<const ColumnInt64&>(*(new_block.get_by_position(2)._column));
//     ASSERT_EQ(4, new_col2.size());
//     ASSERT_EQ(2, new_col2[0]);
//     ASSERT_EQ(20, new_col2[1]);
//     ASSERT_EQ(2, new_col2[2]);
//     ASSERT_EQ(20, new_col2[3]);
//     const ColumnFloat64& new_col3 = reinterpret_cast<const ColumnFloat64&>(*(new_block.get_by_position(3)._column));
//     ASSERT_EQ(4, new_col3.size());
//     ASSERT_EQ(3.1, new_col3[0]);
//     ASSERT_EQ(30.1, new_col3[1]);
//     ASSERT_EQ(3.1, new_col3[2]);
//     ASSERT_EQ(30.1, new_col3[3]);
//     const ColumnFloat64& new_col4 = reinterpret_cast<const ColumnFloat64&>(*(new_block.get_by_position(4)._column));
//     ASSERT_EQ(4, new_col4.size());
//     ASSERT_EQ(4.1, new_col4[0]);
//     ASSERT_EQ(40.1, new_col4[1]);
//     ASSERT_EQ(4.1, new_col4[2]);
//     ASSERT_EQ(40.1, new_col4[3]);
//     const ColumnString& new_col5 = reinterpret_cast<const ColumnString&>(*(new_block.get_by_position(5)._column));
//     ASSERT_EQ(4, new_col5.size());
//     ASSERT_EQ("5", new_col5[0]);
//     ASSERT_EQ("55", new_col5[1]);
//     ASSERT_EQ("5", new_col5[2]);
//     ASSERT_EQ("55", new_col5[3]);
// }
//
// }
//
