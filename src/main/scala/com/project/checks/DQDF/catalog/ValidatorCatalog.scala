package com.project.checks.DQDF.catalog

//This catalog type contains validation-specific
//information that DQDF uses to optimize the overall data quality
//  computation. A validator catalog contains a modified validator’s
//checker function, a validity report, and a trigger function. A validator
//  catalog can also include a list of independent operations
//used as part of its checker function. A trigger function is specific
//to a validator. For example, a validator that only operates on a
//numerical column would have a trigger function that returns false
//if a string column has been dropped. However, a trigger function
//of a dataset-based validator will likely return true if there is any
//changes in the data.

class ValidatorCatalog {

}
