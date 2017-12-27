package com.loader;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

public class Loader {

    public static void main(String[] args) {

        try {
            //第二种
            URL url = new URL("file:D:/jarload/test.jar");
            URLClassLoader myClassLoader = new URLClassLoader(new URL[] { url }, Thread.currentThread().getContextClassLoader());
            Class<?> myClass = myClassLoader.loadClass("com.java.jarloader.TestAction");
//            InterfaceAction action1 = (InterfaceAction) myClass.newInstance();
//            String str1 = action1.action();
//            System.out.println(str1);
//        } catch (IllegalAccessException e1) {
//            e1.printStackTrace();
//        } catch (InstantiationException e1) {
//            e1.printStackTrace();
        } catch (MalformedURLException e1) {
            e1.printStackTrace();
        } catch (ClassNotFoundException e1) {
            e1.printStackTrace();
        }

    }
}
