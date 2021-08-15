package org.lealone.hansql.exec.planner.cost;

import org.lealone.hansql.exec.physical.base.GroupScan;

/**
 * PluginCost describes the cost factors to be used when costing for the specific storage/format plugin
 */
public interface PluginCost {
  org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PluginCost.class);

  /**
   * An interface to check if a parameter provided by user is valid or not.
   * @param <T> Type of the parameter.
   */
  interface CheckValid<T> {
    boolean isValid(T paramValue);
  }

  /**
   * Class which checks whether the provided parameter value is greater than
   * or equals to a minimum limit.
   */
  class greaterThanEquals implements CheckValid<Integer> {
    private final Integer atleastEqualsTo;
    public greaterThanEquals(Integer atleast) {
      atleastEqualsTo = atleast;
    }

    @Override
    public boolean isValid(Integer paramValue) {
      if (paramValue >= atleastEqualsTo &&
          paramValue <= Integer.MAX_VALUE) {
        return true;
      } else {
        logger.warn("Setting default value as the supplied parameter value is less than {}", paramValue);
        return false;
      }
    }
  }

  /**
   * @return the average column size in bytes
   */
  int getAverageColumnSize(GroupScan scan);

  /**
   * @return the block size in bytes
   */
  int getBlockSize(GroupScan scan);

  /**
   * @return the sequential block read cost
   */
  int getSequentialBlockReadCost(GroupScan scan);

  /**
   * @return the random block read cost
   */
  int getRandomBlockReadCost(GroupScan scan);
}
