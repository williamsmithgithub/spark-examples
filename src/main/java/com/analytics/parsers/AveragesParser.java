package com.analytics.parsers;

import java.io.Serializable;
import org.joda.time.DateTime;

/**
 *
 * @author hadoop
 */
public class AveragesParser implements Serializable {

    private StringBuilder sb;
    private Long latency;

    public AveragesParser() {
    }

    public AveragesParser(StringBuilder sb) {
        this.sb = sb;
    }

    public StringBuilder Parse(String line) {
        if (!line.equals("") && line != null) {
            String[] field = line.split(" ");
            String httpCode = field[9];
            if (httpCode.startsWith("2")) {
                String timeStamp = field[14];
                String accountAndGroup = field[17];
                String group = accountAndGroup.split("-")[1];
                String latencyMs = field[21];

                DateTime dt = new DateTime(Long.parseLong(timeStamp));
                int yr = dt.getYear();
                int mnth = dt.getMonthOfYear();
                int dayMnth = dt.getDayOfMonth();
                int hrDay = dt.getHourOfDay();
                String compKey = group + ":" + yr + ":" + mnth + ":" + dayMnth + ":" + hrDay;
                this.sb.append(compKey);
                this.setLatency(Long.parseLong(latencyMs));
            }
        }
        return this.sb;
    }

    public StringBuilder getSb() {
        return sb;
    }

    public void setSb(StringBuilder sb) {
        this.sb = sb;
    }

    public Long getLatency() {
        return latency;
    }

    public void setLatency(Long latency) {
        this.latency = latency;
    }

}
