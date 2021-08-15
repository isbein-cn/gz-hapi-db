/*
#***********************************************
#
#      Filename: gz-hapi-db/lib/index.js
#
#        Author: wwj - 318348750@qq.com
#       Company: 甘肃国臻物联网科技有限公司
#   Description: hapi框架数据库组件
#        Create: 2021-08-15 15:44:25
# Last Modified: 2021-08-16 00:25:14
#***********************************************
*/
'use strict'

const Knex = require('knex')
const Joi = require('joi')
const Hoek = require('@hapi/hoek')

// Declare internals

const internals = {
  schema: Joi.object({
    name: Joi.string().default('default').required(),
    alias: Joi.array().items(Joi.string()).single(),
    client: Joi.string().required(),
    connection: Joi.object().required(),
    pool: Joi.object({
      min: Joi.number().integer().default(0),
      max: Joi.number().integer().default(10),
      acquireTimeoutMillis: Joi.number().integer(),
      createTimeoutMillis: Joi.number().integer(),
      idleTimeoutMillis: Joi.number().integer(),
      reapIntervalMillis: Joi.number().integer(),
      createRetryIntervalMillis: Joi.number().integer(),
      propagateCreateError: Joi.boolean()
    }),
    migrations: Joi.object({
      auto: Joi
        .boolean()
        .truthy('yes', 'true', '1', 1)
        .falsy('no', 'false', '0', 0),
      directory: Joi.string(),
      tableName: Joi.string()
    }),
    postProcessResponse: Joi.func(),
    wrapIdentifier: Joi.func(),
    acquireConnectionTimeout: Joi.number().integer(),
    useNullAsDefault: Joi.boolean(),
    snakeCaseMapping: Joi.boolean()
  }),
  defaults: {
    name: 'default',
    connection: {},
    snakeCaseMapping: false
  }
}

internals.kServer = Symbol('server')

/**
 * Db instance selector
 * @param {string} name
 * @return {object}
 */
internals.db = function (name = 'default') {
  Hoek.assert(this.has(name), `Database connection "${name}" not found`)
  return this.get(name)
}

/**
 * Db配置
 * @param {object} opts
 * @return {object}
 */
internals.provision = async function (opts) {
  // 配置与默认配置合并
  opts = Hoek.applyToDefaults(internals.defaults, opts)

  // 验证无效会抛出错误
  const settings =
    Joi.attempt(opts, internals.schema, '无效的数据库配置')

  // 已有连接会抛出错误
  Hoek.assert(
    !this.has(settings.name),
    `数据库连接已存在: "${settings.name}"`
  )

  const knexOpts = Hoek.clone(
      settings, {
        shallow: [
          'client',
          'pool',
          'connection',
          'wrapIdentifier',
          'postProcessResponse',
          'migrations'
        ]
      }
    )

  Object.entries(knexOpts.connection).forEach(([k, v]) => {
    if (v === null || v === undefined) {
      delete knexOpts.connection[k]
    }
  })

  if (['mysql2', 'mysql'].includes(knexOpts.client)) {
    knexOpts.connection.timezone = knexOpts.connection.timezone || 'UTC'
    if (knexOpts.connection.typeCast || knexOpts.connection.typeCast === undefined) {
      knexOpts.connection.typeCast = (field, next) => {
        switch (field.type) {
          case 'TINY':
            // convert TINYINT(1) -> Boolean
            if (field.length === 1) {
              return field.string() === '1'
            }
            break
          // case 'DATE':
          // case 'DATETIME':
          // case 'TIMESTAMP':
          //   const date = field.string()
          //   // convert * -> Date
          //   return (date !== null) ? new Date(date) : date
        }
        return next()
      }
    }
  }
  if (settings.snakeCaseMapping) {
    const { postProcessResponse, wrapIdentifier } = internals.snakeCaseMappers()

    if (knexOpts.postProcessResponse) {
      const postProcessResponseCustom = knexOpts.postProcessResponse
      knexOpts.postProcessResponse = (result, queryContext) => postProcessResponseCustom(postProcessResponse(result, queryContext), queryContext)
    } else {
      knexOpts.postProcessResponse = postProcessResponse
    }
    if (knexOpts.wrapIdentifier) {
      const wrapIdentifierCustom = knexOpts.wrapIdentifier
      knexOpts.wrapIdentifier = (value, origImpl, queryContext) => wrapIdentifierCustom(value, (v) => wrapIdentifier(v, origImpl, queryContext), queryContext)
    } else {
      knexOpts.wrapIdentifier = wrapIdentifier
    }
  }
  const knex = new Knex(knexOpts)
  if (knexOpts.migrations && knexOpts.migrations.auto) {
    this[internals.kServer].ext({
      type: 'onPreStart',
      method: async () => {
        await knex.migrate.latest()
        this[internals.kServer].log(['db', 'migration', 'info'], 'Database successful migrated to the latest version')
      }
    })
  }

  // ping
  try {
    await knex.raw('/* ping */ SELECT 1')
  } catch (err) {
    this[internals.kServer].log(['db', 'error'], err.sqlMessage || err.message)
    throw new Error(`Database error: ${err.sqlMessage || err.message}`)
  }

  this.set(settings.name, knex)
  if (settings.alias) {
    settings.alias.filter((v) => !this.has(v)).forEach((name) => {
      this.set(name, knex)
    })
  }

  return knex
}

/**
 * Sets the default instance
 * @param {string} name
 */
internals.default = function (name) {
  Hoek.assert(this.has(name), `Database connection "${name}" not found`)
  this.set('default', this.get(name))
}

/**
 * memoize函数,缓存函数计算结果
 * @params {Function} func 需要缓存结果的函数
 */
internals.memoize = (func) => {
  const cache = new Map()

  return input => {
    let output = cache.get(input)

    if (output === undefined) {
      output = func(input)
      cache.set(input, output)
    }

    return output
  }
}

// camelCase to snake_case converter that also works with
// non-ascii characters.
internals.snakeCase = (str) => {
  if (str.length === 0) {
    return str
  }

  const upper = str.toUpperCase()
  const lower = str.toLowerCase()

  let out = lower[0]

  for (let i = 1, l = str.length; i < l; ++i) {
    const char = str[i]
    const prevChar = str[i - 1]

    const upperChar = upper[i]
    const prevUpperChar = upper[i - 1]

    const lowerChar = lower[i]
    const prevLowerChar = lower[i - 1]

    // Test if `char` is an upper-case character and that the character
    // actually has different upper and lower case versions.
    if (char === upperChar && upperChar !== lowerChar) {
      // Multiple consecutive upper case characters shouldn't add underscores.
      // For example "fooBAR" should be converted to "foo_bar".
      if (prevChar === prevUpperChar && prevUpperChar !== prevLowerChar) {
        out += lowerChar
      } else {
        out += '_' + lowerChar
      }
    } else {
      out += char
    }
  }

  return out
}

// snake_case to camelCase converter that simply reverses
// the actions done by `snakeCase` function.
internals.camelCase = (str) => {
  if (str.length === 0) {
    return str
  }

  let out = str[0]

  for (let i = 1, l = str.length; i < l; ++i) {
    const char = str[i]
    const prevChar = str[i - 1]

    if (char !== '_') {
      if (prevChar === '_') {
        out += char.toUpperCase()
      } else {
        out += char
      }
    }
  }

  return out
}

// Returns a function that splits the inputs string into pieces using `separator`,
// only calls `mapper` for the last part and concatenates the string back together.
// If no separators are found, `mapper` is called for the entire string.
internals.mapLastPart = (mapper, separator) => (str) => {
  const idx = str.lastIndexOf(separator)
  const mapped = mapper(str.slice(idx + separator.length))
  return str.slice(0, idx + separator.length) + mapped
}

// Returns a function that takes an object as an input and maps the object's keys
// using `mapper`. If the input is not an object, the input is returned unchanged.
internals.keyMapper = (mapper) => (obj) => {
  if (!obj || typeof obj !== 'object' || Array.isArray(obj)) {
    return obj
  }

  const keys = Object.keys(obj)
  const out = {}

  for (let i = 0, l = keys.length; i < l; ++i) {
    const key = keys[i]
    out[mapper(key)] = obj[key]
  }
  return out
}

internals.snakeCaseMappers = () => ({
  parse: internals.keyMapper(internals.memoize(internals.camelCase)),
  format: internals.keyMapper(internals.memoize(internals.snakeCase))
})

internals.identifierMappers = ({ parse, format, idSeparator = ':' } = {}) => {
  const formatId = internals.memoize(internals.mapLastPart(format, idSeparator))
  const parseId = internals.memoize(internals.mapLastPart(parse, idSeparator))
  const parseKeys = internals.keyMapper(parseId)

  return {
    wrapIdentifier (identifier, origWrap) {
      return origWrap(formatId(identifier))
    },

    postProcessResponse (result) {
      if (Array.isArray(result)) {
        const output = new Array(result.length)

        for (let i = 0, l = result.length; i < l; ++i) {
          output[i] = parseKeys(result[i])
        }

        return output
      } else {
        return parseKeys(result)
      }
    }
  }
}

internals.snakeCaseMappers = () => internals.identifierMappers({
  parse: internals.camelCase,
  format: internals.snakeCase
})

module.exports = {
  pkg: require('../package.json'),
  once: true,
  register: async (server, options) => {
    const instances = new Map()
    const db = internals.db.bind(instances)
    //
    instances[internals.kServer] = server

    db.provision = internals.provision.bind(instances)
    db.default = internals.default.bind(instances)

    server.decorate('server', 'db', db)

    if (options.db) {
      if (Array.isArray(options.db)) {
        await Promise.all(options.db.map(async (opts) => server.db.provision(opts)))
      } else {
        await server.db.provision(options.db)
      }
    }
  }
}
