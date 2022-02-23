package com.ververica.field.util;

import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyObject;

import java.util.HashMap;
import java.util.Map;

public class GroovyUtils {

  public static Map<String, GroovyObject> passedClassMap = new HashMap<>();

  public static GroovyClassLoader groovyClassLoader;



  public static GroovyObject loadScript(String script) {
    GroovyObject groovyObject = passedClassMap.get(CryptUtils.md5(script));
    if (groovyObject == null) {
      Class groovyClass = groovyClassLoader.parseClass(script);
      try {
        groovyObject = (GroovyObject) groovyClass.newInstance();
        passedClassMap.put(CryptUtils.md5(script), groovyObject);
      } catch (InstantiationException e) {
        e.printStackTrace();
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }
    }

    return groovyObject;
  }

  public static Object invokeMethod(GroovyObject object, String method, Object[] args) {
    return object.invokeMethod(method, args);
  }

  public static Object invokeMethod(String script, String method, Object[] args) {
    GroovyObject groovy = loadScript(script);
    if (groovy != null) {
      return invokeMethod(groovy, method, args);
    } else {
      return null;
    }
  }

}
