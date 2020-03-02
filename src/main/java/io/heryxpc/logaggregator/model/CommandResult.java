package io.heryxpc.logaggregator.model;

public class CommandResult {

    private Object result;

    public CommandResult() {
    }

    public Object getResult() {
        return result;
    }

    public void setResult(Object result) {
        this.result = result;
    }

    public boolean isEmpty() {
        return result == null;
    }
}
