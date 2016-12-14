//
// Created by Harunobu Daikoku on 2015/11/20.
//

#ifndef SLAVERDD_WORD_COUNT_REDUCER_H
#define SLAVERDD_WORD_COUNT_REDUCER_H

#include <string>
#include <reducer.h>

class WordCountReducer: public Reducer<std::string, int> {
 public:

  virtual std::pair<std::string, int> Reduce(const std::string &key, const std::vector<int> &values) const override;

};

extern "C" std::unique_ptr<Reducer<std::string, int>> Create() {
  return std::unique_ptr<Reducer<std::string, int>>(new WordCountReducer);
}


#endif //SLAVERDD_WORD_COUNT_REDUCER_H
