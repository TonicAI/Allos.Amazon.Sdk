using System.Runtime.CompilerServices;

namespace Allos.Amazon.Sdk;

internal static class ConvertExtensions
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int ToInt32(this uint unsignedInt) => Convert.ToInt32(unsignedInt);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static uint ToUInt32(this int integer) => (uint) integer;
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static long ToInt64(this ulong unsignedLong) => Convert.ToInt64(unsignedLong);
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static ulong ToUInt64(this long longInteger) => (ulong) longInteger;
}