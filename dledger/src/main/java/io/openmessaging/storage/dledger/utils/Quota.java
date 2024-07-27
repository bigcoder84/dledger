/*
 * Copyright 2017-2022 The DLedger Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger.utils;

public class Quota {

    /**
     * 在指定时间窗口内允许的最大日志条目数量。
     */
    private final int max;

    /**
     * 一个数组，用于存储每个时间窗口的日志条目数量。
     */
    private final int[] samples;
    /**
     * 一个数组，记录每个时间窗口的开始时间（秒）。
     */
    private final long[] timeVec;
    /**
     * 窗口数量
     */
    private final int window;

    public Quota(int max) {
        this(5, max);
    }
    public Quota(int window, int max) {
        if (window < 5) {
            window = 5;
        }
        this.max = max;
        this.window = window;
        this.samples = new int[window];
        this.timeVec = new long[window];
    }

    private int index(long currTimeMs) {
        return  (int) (second(currTimeMs) % window);
    }

    private long second(long currTimeMs) {
        return currTimeMs / 1000;
    }

    /**
     * 记录当前时间窗口的日志条目数量。如果当前时间窗口是新的（即timeVec中的记录不是当前秒），则重置该窗口的计数；如果是同一时间窗口，则累加日志条目数量
     * @param value
     */
    public void sample(int value) {
        long timeMs = System.currentTimeMillis();
        int index = index(timeMs);
        long second = second(timeMs);
        if (timeVec[index] != second) {
            timeVec[index] = second;
            samples[index] = value;
        } else {
            samples[index] += value;
        }

    }

    public boolean validateNow() {
        long timeMs = System.currentTimeMillis();
        int index = index(timeMs);
        long second = second(timeMs);
        if (timeVec[index] == second) {
            return samples[index] >= max;
        }
        return false;
    }

    public int leftNow() {
        long timeMs = System.currentTimeMillis();
        return (int) ((second(timeMs) + 1) * 1000 - timeMs);
    }
}
