package io.s4.latin.adapter;

import io.s4.listener.EventHandler;

public interface ISource {
    public void addHandler(EventHandler handler);
    public boolean removeHandler(EventHandler handler);
    public void init();

}
