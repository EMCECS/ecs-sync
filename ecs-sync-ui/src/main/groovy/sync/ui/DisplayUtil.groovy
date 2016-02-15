package sync.ui

import groovy.time.Duration

class DisplayUtil {
    static String simpleSize(Long bytes) {
        if (!bytes) return '0'
        Long base = 1024L
        Integer decimals = 1
        def prefix = ['', 'K', 'M', 'G', 'T']
        int i = Math.log(bytes) / Math.log(base) as Integer
        i = (i >= prefix.size() ? prefix.size() - 1 : i)
        return Math.round((bytes / base**i) * 10**decimals) / 10**decimals + prefix[i]
    }

    static String shortDur(Duration dur) {
        def str = ''
        ['years', 'months', 'days', 'hours', 'minutes', 'seconds'].each {
            if (dur.properties[it]) str += ", ${dur.properties[it]} ${it}"
        }
        str.length() > 2 ? str[2..-1] : str
    }
}
