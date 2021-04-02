package com.umxwe.common.param;

/**
 * @ClassName ParamInfoFactory
 * @Description Todo
 * @Author owen(umxwe))
 * @Date 2020/12/16
 */

/**
 * Factory to create ParamInfo, all ParamInfos should be created via this class.
 */
public class ParamInfoFactory {
    /**
     * Returns a ParamInfoBuilder to configure and build a new ParamInfo.
     *
     * @param name       name of the new ParamInfo
     * @param valueClass value class of the new ParamInfo
     * @param <V>        value type of the new ParamInfo
     * @return a ParamInfoBuilder
     */
    public static <V> ParamInfoBuilder<V> createParamInfo(String name, Class<V> valueClass) {
        return new ParamInfoBuilder<>(name, valueClass);
    }

    /**
     * Builder to build a new ParamInfo. Builder is created by ParamInfoFactory with name and
     * valueClass set.
     *
     * @param <V> value type of the new ParamInfo
     */
    public static class ParamInfoBuilder<V> {
        private String name;
        private String[] alias = new String[0];
        private String description;
        private boolean isOptional = true;
        private boolean hasDefaultValue = false;
        private V defaultValue;
        private ParamValidator<V> validator;
        private Class<V> valueClass;

        ParamInfoBuilder(String name, Class<V> valueClass) {
            this.name = name;
            this.valueClass = valueClass;
        }

        /**
         * Sets the aliases of the parameter.
         *
         * @return the builder itself
         */
        public ParamInfoBuilder<V> setAlias(String[] alias) {
            this.alias = alias;
            return this;
        }

        /**
         * Sets the description of the parameter.
         *
         * @return the builder itself
         */
        public ParamInfoBuilder<V> setDescription(String description) {
            this.description = description;
            return this;
        }

        /**
         * Sets the flag indicating the parameter is optional. The parameter is optional by
         * default.
         *
         * @return the builder itself
         */
        public ParamInfoBuilder<V> setOptional() {
            this.isOptional = true;
            return this;
        }

        /**
         * Sets the flag indicating the parameter is required.
         *
         * @return the builder itself
         */
        public ParamInfoBuilder<V> setRequired() {
            this.isOptional = false;
            return this;
        }

        /**
         * Sets the flag indicating the parameter has default value, and sets the default value.
         *
         * @return the builder itself
         */
        public ParamInfoBuilder<V> setHasDefaultValue(V defaultValue) {
            this.hasDefaultValue = true;
            this.defaultValue = defaultValue;
            return this;
        }

        /**
         * Sets the validator to validate the parameter value set by users.
         *
         * @return the builder itself
         */
        public ParamInfoBuilder<V> setValidator(ParamValidator<V> validator) {
            this.validator = validator;
            return this;
        }

        /**
         * Builds the defined ParamInfo and returns it. The ParamInfo will be immutable.
         *
         * @return the defined ParamInfo
         */
        public ParamInfo<V> build() {
            if (validator != null) {
                validator.setParamName(this.name);
            }
            return new ParamInfo<>(name, alias, description, isOptional, hasDefaultValue,
                    defaultValue, validator, valueClass);
        }
    }

}