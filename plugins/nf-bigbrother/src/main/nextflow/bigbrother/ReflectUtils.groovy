package nextflow.bigbrother

class ReflectUtils {

    static Object bypassProtected(Object obj, String memberName, Object... args) {
        def method = obj.class.declaredMethods.find {
            it.name == memberName && it.parameterCount == args.length
        }
        if (method) {
            method.accessible = true
            return method.invoke(obj, args)
        }
        def field = obj.class.declaredFields.find { it.name == memberName }
        if (field) {
            field.accessible = true
            return field.get(obj)
        }
        def prop = obj.metaClass.hasProperty(obj, memberName)
        if (prop) {
            return obj."$memberName"
        }
        throw new MissingPropertyException(memberName, obj.class)
    }

}
