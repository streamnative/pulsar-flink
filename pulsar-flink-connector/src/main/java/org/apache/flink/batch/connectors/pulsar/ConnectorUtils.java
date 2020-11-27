/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.batch.connectors.pulsar;

import org.apache.pulsar.common.policies.data.OffloadPolicies;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Properties;

/**
 * Utilities in constructing CachedClients.
 */
public class ConnectorUtils {

    /**
     * Create an instance of <code>userClassName</code> using provided <code>classLoader</code>.
     * This instance should implement the provided interface <code>xface</code>.
     *
     * @param userClassName user class name
     * @param xface the interface that the reflected instance should implement
     * @param classLoader class loader to load the class.
     * @return the instance
     */
    public static <T> T createInstance(String userClassName,
                                       Class<T> xface,
                                       ClassLoader classLoader) {
        Class<?> theCls;
        try {
            theCls = Class.forName(userClassName, true, classLoader);
        } catch (ClassNotFoundException | NoClassDefFoundError cnfe) {
            throw new RuntimeException("User class must be in class path", cnfe);
        }
        if (!xface.isAssignableFrom(theCls)) {
            throw new RuntimeException(userClassName + " not " + xface.getName());
        }
        Class<T> tCls = (Class<T>) theCls.asSubclass(xface);
        try {
            Constructor<T> meth = tCls.getDeclaredConstructor();
            return meth.newInstance();
        } catch (InstantiationException ie) {
            throw new RuntimeException("User class must be concrete", ie);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("User class must have a no-arg constructor", e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("User class must a public constructor", e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException("User class constructor throws exception", e);
        }
    }

    public static OffloadPolicies getProperties(Map<String, String> configMap) {
        Properties properties = new Properties();
        for (Map.Entry<String, String> entry : configMap.entrySet()) {
            properties.setProperty(entry.getKey(), entry.getValue());
        }
        return OffloadPolicies.create(properties);
    }

}
