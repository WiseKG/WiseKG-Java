package org.linkeddatafragments.costmodel.impl;

import org.linkeddatafragments.costmodel.ICostModel;
import org.linkeddatafragments.datasource.IDataSource;

import javax.servlet.http.HttpServletRequest;

public class ConstantCostModel implements ICostModel {
    @Override
    public double cost(HttpServletRequest request, IDataSource datasource) {
        return 0.5;
    }
}
