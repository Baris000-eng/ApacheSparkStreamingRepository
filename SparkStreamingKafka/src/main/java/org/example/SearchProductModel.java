package org.example;

/**
 * Hello world!
 *
 */
public class SearchProductModel {
    private String productName;
    private String time;

    public SearchProductModel(String productName, String time){
        this.productName = productName;
        this.time = time;
    }

    public SearchProductModel() {

    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }
}
