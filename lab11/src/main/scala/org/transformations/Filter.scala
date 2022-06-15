package org.transformations

import org.case_class.Diamonds

class Filter  extends java.io.Serializable{

  def expensive(diamonds: Diamonds, value: Double): Boolean = {
    return Diamonds.price > value;
  }

}
