package org.linkeddatafragments.costmodel;

import org.linkeddatafragments.datasource.IDataSource;

import javax.servlet.http.HttpServletRequest;

public interface ICostModel {
    /*
     * Calculates the cost of a request
     * @param request The HTTP request
     * @param datasource The data source
     * @return Cost
     */
    double cost(HttpServletRequest request, IDataSource datasource);
}
