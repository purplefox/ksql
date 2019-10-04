/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function;

import com.google.common.primitives.Primitives;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import java.lang.reflect.Method;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.lang.model.element.Modifier;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

@SuppressWarnings("unused") // Used from generated code
public final class UdtfTemplate {

  private UdtfTemplate() {
  }

  static String generateCode(
      final Method udtfMethod,
      final String className,
      final String udtfName,
      final String description) {

    final TypeSpec.Builder udtfTypeSpec = TypeSpec.classBuilder(className);
    udtfTypeSpec.addModifiers(Modifier.PUBLIC);

    udtfTypeSpec.superclass(BaseTableFunction.class);

    udtfTypeSpec.addMethod(
        MethodSpec.constructorBuilder()
            .addModifiers(Modifier.PUBLIC)
            .addParameter(ParameterizedTypeName.get(List.class, Schema.class), "args")
            .addParameter(Schema.class, "outputType")
            .addStatement("super($S, outputType, args, $S)",
                udtfName,
                description)
            .build());

    final String udtfArgs = IntStream.range(0, udtfMethod.getParameterTypes().length)
        .mapToObj(i -> String.format("(%s) coerce(args, %s.class, %d)",
            Primitives.wrap(udtfMethod.getParameterTypes()[i]).getName(),
            udtfMethod.getParameterTypes()[i].getName(),
            i))
        .collect(Collectors.joining(", "));

    // TODO fix this

    udtfTypeSpec.addMethod(
        MethodSpec.methodBuilder("getInstance")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(TableFunctionArguments.class, "args", Modifier.FINAL)
            .addStatement("args.ensureArgCount($L, $S)", udtfMethod.getParameters().length + 1,
                udtfName)
            .returns(KsqlTableFunction.class)
            .addStatement(
                "return new $L($T.$L($L), getArguments(), getAggregateType(),"
                    + " getReturnType(), aggregateSensor, mapSensor, mergeSensor)",
                className,
                udtfMethod.getDeclaringClass(),
                udtfMethod.getName(),
                udtfArgs)
            .build());

    return JavaFile.builder("io.confluent.ksql.function.udtf", udtfTypeSpec.build())
        .addStaticImport(UdtfTemplate.class, "coerce")
        .build()
        .toString();
  }

  @SuppressWarnings("unchecked")
  public static <T> T coerce(
      final AggregateFunctionArguments args,
      final Class<?> clazz,
      final int index) {
    if (Integer.class.isAssignableFrom(Primitives.wrap(clazz))) {
      return (T) Integer.valueOf(args.arg(index + 1));
    } else if (Long.class.isAssignableFrom(Primitives.wrap(clazz))) {
      return (T) Long.valueOf(args.arg(index + 1));
    } else if (Double.class.isAssignableFrom(Primitives.wrap(clazz))) {
      return (T) Double.valueOf(args.arg(index + 1));
    } else if (Float.class.isAssignableFrom(Primitives.wrap(clazz))) {
      return (T) Float.valueOf(args.arg(index + 1));
    } else if (Byte.class.isAssignableFrom(Primitives.wrap(clazz))) {
      return (T) Byte.valueOf(args.arg(index + 1));
    } else if (Short.class.isAssignableFrom(Primitives.wrap(clazz))) {
      return (T) Short.valueOf(args.arg(index + 1));
    } else if (Boolean.class.isAssignableFrom(Primitives.wrap(clazz))) {
      return (T) Boolean.valueOf(args.arg(index + 1));
    } else if (String.class.isAssignableFrom(clazz)) {
      return (T) args.arg(index + 1);
    } else if (Struct.class.isAssignableFrom(clazz)) {
      return (T) args.arg(index + 1);
    }

    throw new KsqlFunctionException("Unsupported udaf argument type: " + clazz);
  }

}
