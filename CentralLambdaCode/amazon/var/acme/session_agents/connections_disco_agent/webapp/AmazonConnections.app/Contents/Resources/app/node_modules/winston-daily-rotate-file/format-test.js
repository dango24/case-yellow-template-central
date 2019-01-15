const {createLogger, format, transports} = require('winston');
require('.');

/**
 * Filters logs by levels.
 * @param {string|string[]} level - String or array of allowed levels
 */
function levelsFilter(level) {
    return format(info => {
        if (typeof level === 'string') {
            if (info.level === level) {
                return info;
            }
        } else if (Array.isArray(level)) {
            if (level.includes(info.level)) {
                return info;
            }
        }
        return false;
    })();
}

const logger = createLogger({
    transports: [
        // Console transport, for comparison
        new transports.Console({
            level: 'silly',
            format: format.combine(
                levelsFilter(['silly', 'debug']),
                format.timestamp({format: 'YYYY-MM-DDTHH:mm:ss.sss'})
            )
        }),
        // File transport, for comparison
        new transports.File({
            dirname: 'logs',
            filename: `myService-native.log`,
            level: 'silly',
            format: format.combine(
                levelsFilter(['silly', 'debug']),
                format.timestamp({format: 'YYYY-MM-DDTHH:mm:ss.sss'})
            )
        }),
        // DailyRotateFile transport, which logs `undefined` on falsey value.
        new transports.DailyRotateFile({
            dirname: 'logs',
            filename: `%DATE%-myService-debug.log`,
            maxFiles: '10d',
            zippedArchives: true,
            level: 'silly',
            format: format.combine(
                levelsFilter(['silly', 'debug']),
                format.timestamp({format: 'YYYY-MM-DDTHH:mm:ss.sss'})
            )
        })
    ]
});

// Log some things :
logger.debug('Debug log');
logger.info('Info log'); // Shouldn't be displayed
logger.error('Error log'); // Shouldn't be displayed
logger.silly('Silly log');
