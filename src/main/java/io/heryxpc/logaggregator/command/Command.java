package io.heryxpc.logaggregator.command;

import io.heryxpc.logaggregator.model.CommandResult;

public interface Command {

    CommandResult execute();
}

