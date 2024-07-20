using System;
using System.Collections.Generic;

namespace DeveloperSample.Container
{
    public class Container
    {
        private readonly Dictionary<Type, Type> _typeDict = new();

        public void Bind(Type interfaceType, Type implementationType)
        {
            if (!interfaceType.IsAssignableFrom(implementationType))
            {
                throw new ArgumentException($"interface {interfaceType.FullName} not implemented by Type {implementationType.FullName}");
            }

            _typeDict[interfaceType] = implementationType;
        }

        public T Get<T>()
        {
            return (T)GetImplementation(typeof(T));
        }

        private object GetImplementation(Type type)
        {
            if (!_typeDict.TryGetValue(type, out var implementationType))
            {
                throw new ArgumentException($"{type.FullName} Type is not in container");
            }

            var implementationConstructor = implementationType.GetConstructors()[0];
            var implementationParameters = implementationConstructor.GetParameters();

            if (implementationParameters.Length == 0)
            {
                return Activator.CreateInstance(implementationType);
            }

            var parameterValues = new object[implementationParameters.Length];
            for (int i = 0; i < implementationParameters.Length; i++)
            {
                parameterValues[i] = GetImplementation(implementationParameters[i].ParameterType);
            }

            return implementationConstructor.Invoke(parameterValues);
        }
    }
}
